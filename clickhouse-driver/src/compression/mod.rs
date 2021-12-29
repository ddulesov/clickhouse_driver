use std::io;
use std::os::raw::{c_char, c_int};
use std::pin::Pin;
use std::task::{Context, Poll};

use byteorder::WriteBytesExt;
use byteorder::{LittleEndian, ReadBytesExt};
use lz4::liblz4::LZ4_decompress_safe;
use lz4::liblz4::{LZ4_compressBound, LZ4_compress_default};

use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};

#[cfg(not(feature = "cityhash_rs"))]
use clickhouse_driver_cth::city_hash_128;
#[cfg(feature = "cityhash_rs")]
use clickhouse_driver_cthrs::city_hash_128;

use crate::errors;
use crate::errors::DriverError;
use crate::prelude::CompressionMethod;

pub(crate) struct LZ4CompressionWrapper<W: ?Sized> {
    buf: Vec<u8>,
    inner: W,
}

const LZ4_COMPRESSION_METHOD: u8 = 0x82;

impl<W> LZ4CompressionWrapper<W> {
    pub(crate) fn new(writer: W) -> LZ4CompressionWrapper<W> {
        let buf = Vec::new();
        LZ4CompressionWrapper { buf, inner: writer }
    }
}

impl<W> io::Write for LZ4CompressionWrapper<W>
where
    W: io::Write + ?Sized,
{
    fn flush(&mut self) -> std::result::Result<(), io::Error> {
        let bufsize = unsafe { LZ4_compressBound(self.buf.len() as i32) as usize };

        let mut compressed = vec![0u8; 9 + bufsize];

        let bufsize = unsafe {
            LZ4_compress_default(
                self.buf[..].as_ptr() as *const c_char,
                compressed[9..].as_mut_ptr() as *mut c_char,
                self.buf.len() as i32,
                bufsize as i32,
            )
        };

        if bufsize < 0 {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                DriverError::PacketTooLarge,
            ));
        }
        let original_size = self.buf.len() as u32;

        drop(std::mem::take(&mut self.buf));

        compressed.resize(bufsize as usize + 9, 0);

        let compressed_size = compressed.len() as u32;
        {
            let mut cursor = io::Cursor::new(compressed);
            cursor.write_u8(LZ4_COMPRESSION_METHOD)?;
            cursor.write_u32::<LittleEndian>(compressed_size)?;
            cursor.write_u32::<LittleEndian>(original_size)?;

            let compressed = cursor.into_inner();

            let hash = city_hash_128(&compressed[..]);

            //self.inner.write_all(&*hash)?;
            self.inner.write_u64::<LittleEndian>(hash.0)?;
            self.inner.write_u64::<LittleEndian>(hash.1)?;
            self.inner.write_all(&compressed[..])?;
        }
        self.inner.flush()
    }

    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, io::Error> {
        let bufsize = self.buf.write(buf);
        if self.buf.len() > crate::MAX_BLOCK_SIZE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                DriverError::PacketTooLarge,
            ));
        };
        bufsize
    }
}

#[derive(Debug)]
enum CompressionState {
    /// Read first 16 byte containing hash sum of the block +9 bytes of header
    Hash,
    /// Read raw data from underlying reader
    Compressed,
    /// Supply decompressed data to caller
    Decompressed,
    /// Bypass LZ4 compression. Read right from underlying reader
    ByPass,
}

impl CompressionState {
    #[inline]
    fn is_bypass(&self) -> bool {
        matches!(self, CompressionState::ByPass)
    }
}

fn read_head(buf: &[u8]) -> io::Result<(u32, u32)> {
    let mut cursor = io::Cursor::new(buf);
    cursor.set_position(16);

    let code = cursor.read_u8().expect("");
    let comp_size = cursor.read_u32::<LittleEndian>().expect("");
    let raw_size = cursor.read_u32::<LittleEndian>().expect("");

    if code != LZ4_COMPRESSION_METHOD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            DriverError::BadCompressedPacketHeader,
        ));
    }

    if comp_size == 0
        || comp_size as usize > crate::MAX_BLOCK_SIZE_BYTES
        || raw_size as usize > crate::MAX_BLOCK_SIZE_BYTES
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            errors::DriverError::PacketTooLarge,
        ));
    };

    Ok((comp_size, raw_size))
}

fn decompress(buf: &[u8], raw_size: usize) -> io::Result<Vec<u8>> {
    let calculated_hash = city_hash_128(&buf[16..]);

    if calculated_hash != &buf[0..16] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            errors::DriverError::BadHash,
        ));
    };

    // TODO: decompression in-place
    let orig = vec![0u8; raw_size];

    unsafe {
        let res = {
            LZ4_decompress_safe(
                (buf.as_ptr() as *const c_char).add(16 + 9),
                orig.as_ptr() as *mut c_char,
                (buf.len() - 16 - 9) as c_int,
                raw_size as i32,
            )
        };

        if res < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                errors::DriverError::BadCompressedPacketHeader,
            ));
        }
        debug_assert_eq!(res as usize, raw_size);
    }
    Ok(orig)
}

pub(crate) struct LZ4ReadAdapter<R: AsyncBufRead + ?Sized> {
    /// Internal buffer. It's used alternately for
    /// reading from underlying reader and for storing decompressed data
    data: Vec<u8>,
    state: CompressionState,
    /// Size of raw(decompressed) data
    raw_size: usize,
    /// Number of read or written from(to) `data` bytes
    p: usize,
    inner: R,
}

impl<R: AsyncBufRead + Unpin + Send> LZ4ReadAdapter<R> {
    pub(crate) fn new(reader: R) -> LZ4ReadAdapter<R> {
        let data = vec![0; 16 + 9];
        LZ4ReadAdapter {
            data,
            state: CompressionState::Hash,
            p: 0,
            raw_size: 0,
            inner: reader,
        }
    }
    pub(crate) fn new_with_param(reader: R, compression: CompressionMethod) -> LZ4ReadAdapter<R> {
        if compression == CompressionMethod::LZ4 {
            LZ4ReadAdapter::new(reader)
        } else {
            LZ4ReadAdapter {
                data: Vec::new(),
                state: CompressionState::ByPass,
                p: 0,
                raw_size: 0,
                inner: reader,
            }
        }
    }
    /// Consume adapter buffered uncompressed block data
    #[allow(dead_code)]
    fn into_vec(self) -> Vec<u8> {
        if let CompressionState::Decompressed = self.state {
            self.data
        } else {
            panic!("consume incomplete LZ4 Block");
        }
    }
    /// Get TCP socket reader
    pub(crate) fn inner_ref(&mut self) -> &mut R {
        &mut self.inner
    }

    fn inner_consume(&mut self, amt: usize) {
        self.p += amt;
        // Have reached to the end of the block. Go to the next one
        if self.p >= self.data.len() {
            self.p = 0;
            self.data.resize(16 + 9, 0);
            self.state = CompressionState::Hash;
        }
    }
    /// Read LZ4 compressed block from underlying stream,
    /// make decompression and return slice of raw unread data.
    fn fill(&mut self, cx: &mut Context<'_>) -> Poll<Result<&[u8], io::Error>> {
        loop {
            match self.state {
                // Read decompressed buffer data
                CompressionState::Decompressed => {
                    return Poll::Ready(Ok(&self.data[self.p..]));
                }
                // Read rest of compressed data into own buffer
                CompressionState::Compressed => {
                    let raw_size = self.raw_size;
                    // Read from underlying reader. Bypass buffering
                    let mut buf = ReadBuf::new(self.data[self.p..].as_mut());
                    ready!(Pin::new(&mut self.inner).poll_read(cx, &mut buf)?);
                    self.p += buf.filled().len();
                    // Got to the end. Decompress and return raw buffer
                    if self.p >= self.data.len() {
                        debug_assert_eq!(self.p, self.data.len());
                        self.data = decompress(self.data.as_slice(), raw_size)?;
                        self.p = 0;
                        self.state = CompressionState::Decompressed;
                        return Poll::Ready(Ok(self.data.as_ref()));
                    }
                }
                // Read 16 byte hash + 9 byte header
                CompressionState::Hash => {
                    let buf = ready!(Pin::new(&mut self.inner).poll_fill_buf(cx)?);

                    debug_assert_eq!(self.data.len(), 16 + 9);

                    if self.p == 0 && buf.len() >= (16 + 9) {
                        // Buffered data is long enough, and  we can read header
                        let (comp_size, raw_size) = read_head(buf)?;

                        let raw_size = raw_size as usize;
                        let comp_size = comp_size as usize;

                        // Optimize decompression using underlying buffer as input
                        // We have a LZ4 block in whole in its buffer and can decompress it without copying
                        if buf.len() >= (comp_size + 16) {
                            self.data = decompress(&buf[0..comp_size + 16], raw_size)?;
                            self.p = 0;
                            self.state = CompressionState::Decompressed;

                            Pin::new(&mut self.inner).consume(comp_size + 16);
                            return Poll::Ready(Ok(self.data.as_slice()));
                        } else {
                            // Read block by chunks. First read buffered data
                            self.data.resize(16 + comp_size as usize, 0);
                            debug_assert!(self.data.capacity() >= (comp_size + 16));
                            debug_assert!(self.data.len() == (comp_size + 16));

                            // Copy available len(buf) bytes from underlying stream and consume it
                            self.data[0..buf.len()].copy_from_slice(buf);
                            self.p = buf.len();

                            Pin::new(&mut self.inner).consume(self.p);
                            self.raw_size = raw_size;
                            // Read the rest bytes
                            self.state = CompressionState::Compressed;
                            continue;
                        }
                    } else {
                        // We have less then 25 buffered bytes. Read it and then the rest of the header
                        let n = std::cmp::min(16 + 9 - self.p, buf.len());
                        // Copy n available bytes from underlying stream and consume it
                        self.data[self.p..self.p + n].copy_from_slice(&buf[0..n]);
                        Pin::new(&mut self.inner).consume(n);
                        self.p += n;
                    }
                    // I hope, it must be rare case when to read header require more than 1 call.
                    if self.p >= (16 + 9) {
                        debug_assert_eq!(self.p, 16 + 9);

                        let (comp_size, raw_size) = read_head(self.data.as_slice())?;
                        self.raw_size = raw_size as usize;
                        let comp_size = comp_size as usize;

                        self.data.resize((16 + comp_size) as usize, 0);
                        //self.p = 9 + 16;
                        // Read the rest of LZ4 block without double buffering right from TCP socket
                        self.state = CompressionState::Compressed;
                    }
                }
                // This state does not imply decompression circle
                CompressionState::ByPass => unreachable!(),
            };
        }
    }
}

impl<R: AsyncBufRead + Unpin + Send> AsyncRead for LZ4ReadAdapter<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();

        // println!("req read {} bytes from {}",
        //          buf.len(),
        //          if me.state.is_bypass() {"none"} else { "lz4"}
        // );
        if me.state.is_bypass() {
            return Pin::new(&mut me.inner).poll_read(cx, buf);
        }

        let data = ready!(me.fill(cx)?);
        let ready_to_read = data.len();

        let toread = std::cmp::min(buf.remaining(), ready_to_read);
        //let cz = io::copy(inner, buf)?;

        if toread == 0 {
            return Poll::Ready(Ok(()));
        };

        buf.put_slice(&data[0..toread]);
        me.inner_consume(toread);
        Poll::Ready(Ok(()))
    }
}

impl<R: AsyncBufRead + Unpin + Send> AsyncBufRead for LZ4ReadAdapter<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8], io::Error>> {
        let me = self.get_mut();
        if me.state.is_bypass() {
            return Pin::new(&mut me.inner).poll_fill_buf(cx);
        }
        me.fill(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let me = self.get_mut();
        if me.state.is_bypass() {
            Pin::new(&mut me.inner).consume(amt);
        } else {
            me.inner_consume(amt)
        }
    }
}

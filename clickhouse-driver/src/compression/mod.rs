use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use byteorder::WriteBytesExt;
use byteorder::{LittleEndian, ReadBytesExt};
use tokio::io::{AsyncBufRead, AsyncRead};

#[cfg(not(feature = "cityhash_rs"))]
use clickhouse_driver_cth::city_hash_128;
#[cfg(feature = "cityhash_rs")]
use clickhouse_driver_cthrs::city_hash_128;
pub use clickhouse_driver_lz4::{
    LZ4_Compress, LZ4_CompressBounds, LZ4_Decompress, LZ4_compress_default,
};

use crate::errors;
use crate::errors::DriverError;

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
        let bufsize = LZ4_CompressBounds(self.buf.len());
        let mut compressed: Vec<u8> = Vec::with_capacity(9 + bufsize);
        unsafe {
            compressed.set_len(9 + bufsize);
        }
        let bufsize = LZ4_Compress(&self.buf[..], &mut compressed[9..]);
        if bufsize < 0 {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                DriverError::PacketTooLarge,
            ));
        }
        let original_size = self.buf.len() as u32;

        drop(std::mem::replace(&mut self.buf, Vec::new()));

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

enum CompressionState {
    Hash,
    Compressed,
    Decompressed,
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
            //"unsupported compression method",
        ));
    }

    if comp_size == 0
        || comp_size as usize > crate::MAX_BLOCK_SIZE_BYTES
        || raw_size as usize > crate::MAX_BLOCK_SIZE_BYTES
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            errors::DriverError::PacketTooLarge,
            //"compressed block size exceeds max value",
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
    let mut orig: Vec<u8> = Vec::with_capacity(raw_size);
    unsafe {
        orig.set_len(raw_size);
        let res = LZ4_Decompress(&buf[16 + 9..], &mut orig[..]);
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
    // TODO: optimize out. Use underlying reader buffer to read first  16(hash)+9(head)
    data: Vec<u8>,
    state: CompressionState,
    raw_size: usize,
    p: usize,
    inner: R,
}

impl<R: AsyncBufRead + Unpin + Send> LZ4ReadAdapter<R> {
    pub(crate) fn new(reader: R) -> LZ4ReadAdapter<R> {
        let mut data = Vec::with_capacity(16 + 9);
        unsafe {
            data.set_len(16 + 9);
        }
        LZ4ReadAdapter {
            data,
            state: CompressionState::Hash,
            p: 0,
            raw_size: 0,
            inner: reader,
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

    pub(crate) fn inner_ref(&mut self) -> &mut R {
        &mut self.inner
    }

    fn inner_consume(&mut self, amt: usize) {
        self.p += amt;
        if self.p >= self.data.len() {
            self.p = 0;
            // Next block
            self.data.resize(16 + 9, 0);
            self.state = CompressionState::Hash;
        }
    }

    fn fill(&mut self, cx: &mut Context<'_>) -> Poll<Result<&[u8], io::Error>> {
        loop {
            match self.state {
                CompressionState::Decompressed => {
                    return Poll::Ready(Ok(&self.data[self.p..]));
                }
                CompressionState::Compressed => {
                    // Read from underlying reader. Bypass buffering
                    let raw_size = self.raw_size;

                    let n =
                        ready!(Pin::new(&mut self.inner).poll_read(cx, &mut self.data[self.p..])?);
                    self.p += n;

                    if self.p >= self.data.len() {
                        debug_assert_eq!(self.p, self.data.len());
                        self.data = decompress(self.data.as_slice(), raw_size)?;
                        self.p = 0;
                        self.state = CompressionState::Decompressed;
                        return Poll::Ready(Ok(self.data.as_ref()));
                    }
                }
                // Read 16 byte hash and 9 byte header
                CompressionState::Hash => {
                    let buf = ready!(Pin::new(&mut self.inner).poll_fill_buf(cx)?);

                    debug_assert_eq!(self.data.len(), 16 + 9);

                    if self.p == 0 && buf.len() >= (16 + 9) {
                        // We can read header
                        let (comp_size, raw_size) = read_head(buf)?;

                        let raw_size = raw_size as usize;
                        let comp_size = comp_size as usize;

                        // Optimize decompression using underlying buffer as input
                        // We have the hole block in its buffer and can decompress it without copying
                        if buf.len() >= (comp_size + 16) {
                            self.data = decompress(&buf[0..comp_size + 16], raw_size)?;
                            self.p = 0;
                            self.state = CompressionState::Decompressed;

                            Pin::new(&mut self.inner).consume(comp_size + 16);
                            return Poll::Ready(Ok(self.data.as_slice()));
                        } else {
                            self.data.reserve((comp_size - 9) as usize);
                            unsafe {
                                self.data.set_len(16 + comp_size as usize);
                            }
                            debug_assert!(self.data.capacity() >= (comp_size + 16));
                            debug_assert!(self.data.len() == (comp_size + 16));

                            // Copy available len(buf) bytes from underlying stream and consume it
                            self.data[0..buf.len()].copy_from_slice(buf);
                            self.p = buf.len();

                            //let p = self.p;
                            Pin::new(&mut self.inner).consume(self.p);
                            self.raw_size = raw_size;
                            // Read the rest bytes
                            self.state = CompressionState::Compressed;
                            continue;
                        }
                    } else {
                        // Read the rest of header
                        let n = std::cmp::min(16 + 9 - self.p, buf.len());
                        // Copy n bytes from underlying stream and consume it
                        self.data[self.p..self.p + n].copy_from_slice(&buf[0..n]);
                        Pin::new(&mut self.inner).consume(n);
                        self.p += n;
                    }
                    // I hope, it must be rare case.
                    if self.p >= (16 + 9) {
                        debug_assert_eq!(self.p, 16 + 9);

                        let (comp_size, raw_size) = read_head(self.data.as_slice())?;
                        self.raw_size = raw_size as usize;
                        let comp_size = comp_size as usize;

                        self.data.reserve((comp_size - 9) as usize);
                        unsafe {
                            self.data.set_len(16 + comp_size as usize);
                        }
                        //self.p = 9 + 16;
                        self.state = CompressionState::Compressed;
                    }
                }
            };
        }
    }
}

impl<R: AsyncBufRead + Unpin + Send> AsyncRead for LZ4ReadAdapter<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, io::Error>> {
        let me = self.get_mut();

        let data = ready!(me.fill(cx)?);
        let ready_to_read = data.len();
        let toread = std::cmp::min(buf.len(), ready_to_read);
        //let cz = io::copy(inner, buf)?;

        if toread == 0 {
            return Poll::Ready(Ok(0));
        };
        buf[0..toread].copy_from_slice(&data[0..toread]);

        me.inner_consume(toread);
        Poll::Ready(Ok(toread))
    }
}

impl<R: AsyncBufRead + Unpin + Send> AsyncBufRead for LZ4ReadAdapter<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8], io::Error>> {
        let me = self.get_mut();
        me.fill(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let me = self.get_mut();
        me.inner_consume(amt);
    }
}

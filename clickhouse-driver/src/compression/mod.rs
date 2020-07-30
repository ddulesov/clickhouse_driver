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
    Block(Vec<u8>, u32, u32),
    Raw(Vec<u8>),
}

pub(crate) struct LZ4ReadAdapter<R: AsyncBufRead + ?Sized> {
    // TODO: optimize out. Use underlying reader buffer to read first  16(hash)+9(head)
    hash: [u8; 16],
    state: CompressionState,
    length: usize,
    inner: R,
}

impl<R: AsyncBufRead + Unpin + Send> LZ4ReadAdapter<R> {
    pub(crate) fn new(reader: R) -> LZ4ReadAdapter<R> {
        LZ4ReadAdapter {
            hash: [0u8; 16],
            state: CompressionState::Hash,
            length: 0,
            inner: reader,
        }
    }
    /// Consume adapter buffered uncompressed block data
    #[allow(dead_code)]
    fn into_vec(self) -> Vec<u8> {
        if let CompressionState::Raw(v) = self.state {
            v
        } else {
            panic!("consume incomplete LZ4 Block");
        }
    }

    pub(crate) fn inner_ref(&mut self) -> &mut R {
        &mut self.inner
    }

    fn fill(&mut self, cx: &mut Context<'_>) -> Poll<Result<&[u8], io::Error>> {
        loop {
            match self.state {
                CompressionState::Raw(ref mut v) => {
                    return Poll::Ready(Ok(&v[self.length..]));
                }
                CompressionState::Hash => {
                    self.length +=
                        ready!(Pin::new(&mut self.inner)
                            .poll_read(cx, &mut self.hash[self.length..])?);
                    if self.length >= self.hash.len() {
                        debug_assert_eq!(self.length, 16);

                        let v = unsafe {
                            let mut v = Vec::with_capacity(16);
                            v.set_len(9);
                            v
                        };
                        self.state = CompressionState::Block(v, 0, 0);
                        self.length = 0;
                    }
                }
                CompressionState::Block(ref mut buf, ref mut decompressed, ref mut compressed) => {
                    if *decompressed == 0 {
                        self.length +=
                            ready!(Pin::new(&mut self.inner)
                                .poll_read(cx, &mut buf[self.length..9])?);
                        //read header
                        if self.length >= 9 {
                            debug_assert_eq!(self.length, 9);

                            let mut cursor = io::Cursor::new(buf);
                            let code: u8 = cursor.read_u8().unwrap();
                            if code != LZ4_COMPRESSION_METHOD {
                                //println!("compression {:?}", cursor.into_inner());

                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    DriverError::BadCompressedPacketHeader,
                                    //"unsupported compression method",
                                )));
                            }

                            *compressed = cursor.read_u32::<LittleEndian>().unwrap();
                            *decompressed = cursor.read_u32::<LittleEndian>().unwrap();

                            if *compressed > crate::MAX_BLOCK_SIZE_BYTES as u32
                                || *decompressed > crate::MAX_BLOCK_SIZE_BYTES as u32
                            {
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    errors::DriverError::PacketTooLarge,
                                    //"compressed block size exceeds max value",
                                )));
                            }
                            if *decompressed == 0 {
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    errors::DriverError::BadCompressedPacketHeader,
                                    //"unacceptable compressed block size",
                                )));
                            }
                            let buf = cursor.into_inner();
                            buf.resize(*compressed as usize, 0);
                        };
                    } else {
                        let read = ready!(Pin::new(&mut self.inner)
                            .poll_read(cx, &mut buf[self.length..(*compressed as usize)])?);
                        self.length += read;
                        if self.length >= *compressed as usize {
                            let calculated_hash = city_hash_128(&buf[..]);

                            if calculated_hash != self.hash {
                                self.state = CompressionState::Raw(Vec::new());
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    errors::DriverError::BadHash,
                                )));
                            }
                            self.length = 0;
                            // TODO: decompression in-place
                            let mut orig: Vec<u8> = Vec::with_capacity(*decompressed as usize);
                            unsafe {
                                orig.set_len(*decompressed as usize);
                                let res = LZ4_Decompress(&buf[9..], &mut orig[..]);
                                if res < 0 {
                                    self.state = CompressionState::Raw(Vec::new());
                                    return Poll::Ready(Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        errors::DriverError::BadCompressedPacketHeader,
                                    )));
                                }
                                debug_assert_eq!(res as u32, *decompressed);
                            }

                            self.state = CompressionState::Raw(orig);
                        }
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

        let inner = ready!(me.fill(cx)?);
        let ready_to_read = inner.len();
        let toread = std::cmp::min(buf.len(), ready_to_read);
        //let cz = io::copy(inner, buf)?;
        if toread == 0 {
            return Poll::Ready(Ok(0));
        };
        buf[0..toread].copy_from_slice(&inner[0..toread]);
        me.length += toread;
        // read next block
        if toread == ready_to_read {
            me.state = CompressionState::Hash;
            me.length = 0;
        }

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
        me.length += amt;
    }
}

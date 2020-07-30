use chrono_tz::Tz;
use futures::Future;
use std::borrow::BorrowMut;
use std::marker::Unpin;
use std::pin::Pin;
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::time;

use super::block::{BlockColumn, BlockColumnHeader, BlockInfo, ServerBlock};
use super::code::*;
use super::column::{AsInColumn, EnumColumn, FixedColumn, StringColumn};
use super::encoder::Encoder;
use super::packet::{ProfileInfo, Response};
use super::value::{ValueDate, ValueDateTime, ValueDateTime64, ValueIp4, ValueIp6, ValueUuid};
use super::ServerInfo;
use crate::compression::LZ4ReadAdapter;
use crate::errors::{self, DriverError, Exception, Result, ServerError};
use crate::pool::CompressionMethod;
use crate::protocol::column::{BoxString, LowCardinalityColumn};
use crate::protocol::decoder::ValueReader;
#[cfg(feature = "int128")]
use crate::types::ValueDecimal128;
use crate::types::{parse_type_field, DecimalBits, Field, SqlType, ValueDecimal32, ValueDecimal64};

macro_rules! err {
    ($err: expr) => {
        Err($err.into())
    };
}

const SHARED_WITH_ADDITIONAL_KEY: u64 = 0x0001;
const GLOBAL_DICTIONARY: u64 = 0x0100;
const ADDITIONAL_KEY: u64 = 0x0200;
//const UPDATE_KEY: u64 = 0x0400;

/// Wait future result with timeout
pub(crate) async fn with_timeout<F, T, E>(fut: F, timeout: Duration) -> Result<T>
where
    F: Future<Output = std::result::Result<T, E>>,
    E: Into<errors::Error>,
{
    match time::timeout(timeout, fut).await {
        Ok(Ok(res)) => Ok(res),
        Ok(Err(e)) => err!(e),
        Err(_) => err!(crate::errors::DriverError::ConnectionTimeout),
    }
}

pub(crate) struct CommandSink<W> {
    pub(crate) writer: W,
}

impl<W> CommandSink<W> {
    pub(crate) fn new(writer: W) -> CommandSink<W> {
        CommandSink { writer }
    }
}

impl<W: AsyncWrite + Unpin> CommandSink<W> {
    pub(crate) async fn cancel(&mut self) -> Result<()> {
        let mut buf = Vec::with_capacity(1);
        CLIENT_CANCEL.encode(&mut buf)?;

        self.writer.write(buf.as_ref()).await?;
        Ok(())
    }
}

pub(crate) struct ResponseStream<'a, R: AsyncRead> {
    fuse: bool,
    pub(crate) skip_empty: bool,
    info: &'a mut ServerInfo,
    reader: LZ4ReadAdapter<BufReader<R>>,
    columns: Vec<BlockColumnHeader>,
}

impl<'a, R: AsyncRead + Unpin + Send> ResponseStream<'a, R> {
    pub(crate) fn info_ref(&self) -> &ServerInfo {
        self.info
    }

    pub(crate) fn with_capacity(
        capacity: usize,
        tcpstream: R,
        info: &'a mut ServerInfo,
    ) -> ResponseStream<'a, R> {
        ResponseStream {
            fuse: false,
            skip_empty: true,
            info,
            reader: LZ4ReadAdapter::new(BufReader::with_capacity(capacity, tcpstream)),
            columns: Vec::new(),
        }
    }

    #[inline]
    pub(crate) fn set_fuse(&mut self) {
        self.fuse = true;
        self.info.clear_pending();
    }

    #[inline]
    pub(crate) fn set_pending(&mut self) {
        self.info.set_pending();
    }
    #[inline]
    pub(crate) fn set_deteriorated(&mut self) {
        self.info.set_deteriorated();
    }

    #[inline]
    pub(crate) fn clear_pending(&mut self) {
        self.info.clear_pending();
    }
    #[inline]
    pub(crate) fn is_pending(&self) -> bool {
        self.info.is_pending()
    }
}

impl<'a, R: AsyncRead + Unpin + Send> ResponseStream<'a, R> {
    async fn inner_next(&mut self) -> Result<Option<Response>> {
        if self.fuse {
            return Ok(None);
        }

        let mut code = [0u8; 1];
        loop {
            if 0 == self.reader.inner_ref().read(&mut code[..]).await? {
                return Ok(None);
            }

            match code[0] as u64 {
                SERVER_PONG => {

                    return Ok(Some(Response::Pong));
                }
                SERVER_END_OF_STREAM => {
                    self.set_fuse();
                    break;
                }
                SERVER_PROFILE_INFO => {
                    let _profile = read_profile(self.reader.inner_ref()).await?;
                }
                SERVER_DATA => {
                    // Skip temporary table name
                    let mut rdr = ValueReader::new(self.reader.inner_ref());
                    let l = rdr.read_vint().await?;

                    Pin::new(self.reader.inner_ref()).consume(l as usize);

                    let reader: &mut (dyn AsyncBufRead + Unpin + Send) =
                        if self.info.compression == CompressionMethod::LZ4 {
                            &mut self.reader as &mut (dyn AsyncBufRead + Unpin + Send)
                        } else {
                            self.reader.inner_ref() as &mut (dyn AsyncBufRead + Unpin + Send)
                        };

                    let resp = read_block(reader, &self.columns, self.info.timezone).await?;


                    if let Some(block) = resp {
                        if self.skip_empty && block.rows == 0 {
                            self.skip_empty = false;
                            // Cache block structure for subsequent read_block calls
                            self.columns = block.into_headers();
                            continue;
                        }
                        return Ok(Some(Response::Data(block)));
                    } else {
                        continue;
                    }
                }
                SERVER_EXCEPTION => {
                    self.set_fuse();
                    let ex = read_exception(self.reader.inner_ref()).await?;
                    return err!(ServerError(ex));
                }
                SERVER_HELLO => {
                    self.set_fuse();
                    return Ok(Some(read_hello(self.reader.inner_ref()).await?));
                }
                SERVER_PROGRESS => {
                    let revision = self.info.revision;
                    let (_rows, _bytes, _total) =
                        read_progress(self.reader.inner_ref(), revision).await?;
                }
                code => {
                    self.set_fuse();

                    return err!(DriverError::UnexpectedPacket {
                        code,
                        payload: self.reader.inner_ref().buffer().to_vec(),
                    });
                }
            };
        }

        Ok(None)
    }

    #[inline]
    pub(crate) async fn next(&mut self, timeout: Duration) -> Result<Option<Response>> {
        with_timeout(self.inner_next(), timeout).await
    }
}

async fn read_block_info<R>(reader: R) -> Result<BlockInfo>
where
    R: AsyncBufRead + Unpin,
{
    let mut b = [0u8; 4];

    let mut rdr = ValueReader::new(reader);
    rdr.read_vint().await?; //skip n1
    rdr.as_mut().read_exact(&mut b[0..1]).await?;
    let overflow: bool = b[0] != 0;
    rdr.read_vint().await?;
    rdr.as_mut().read_exact(&mut b[..]).await?;
    rdr.read_vint().await?; //skip n3

    let cols: u64 = rdr.read_vint().await?;
    let rows: u64 = rdr.read_vint().await?;

    if cols > (u16::MAX as u64) || rows > (crate::MAX_BLOCK_SIZE as u64) {
        return err!(DriverError::RowCountTooMany(rows));
    }

    Ok(BlockInfo {
        cols,
        rows,
        overflow,
        bucket: u32::from_le_bytes(b),
    })
}

pub(crate) async fn load_nulls<R: AsyncBufRead + Unpin>(
    mut reader: R,
    rows: u64,
) -> Result<Vec<u8>> {
    let mut nulls: Vec<u8> = Vec::with_capacity(rows as usize);
    unsafe {
        nulls.set_len(rows as usize);
    };
    reader.read_exact(nulls.as_mut_slice()).await?;

    Ok(nulls)
}

async fn load_lowcardinality_string<R>(
    mut reader: R,
    field: &Field,
    rows: u64,
) -> Result<Box<dyn AsInColumn>>
where
    R: AsyncBufRead + Unpin,
{
    if field.sql_type != SqlType::String {
        return err!(DriverError::UnsupportedType(SqlType::LowCardinality));
    }

    // Read version number
    let mut v: u64 = reader.read_u64_le().await?;
    // Only shared key mode supported
    if v != SHARED_WITH_ADDITIONAL_KEY {
        return err!(DriverError::UnsupportedType(SqlType::LowCardinality));
    }
    // Read serialization type
    v = reader.read_u64_le().await?;

    if v & GLOBAL_DICTIONARY == GLOBAL_DICTIONARY {
        return err!(DriverError::UnsupportedType(SqlType::LowCardinality));
    }

    if v & ADDITIONAL_KEY == 0 {
        return err!(DriverError::UnsupportedType(SqlType::LowCardinality));
    }
    let index_type: u8 = v as u8 & 0x0F;

    // Read the number of keys
    v = reader.read_u64_le().await?;

    let keys: FixedColumn<BoxString> =
        FixedColumn::load_string_column(reader.borrow_mut(), v as u64).await?;
    // Read the number of values
    v = reader.read_u64_le().await?;
    if v != rows {
        return err!(DriverError::BrokenData);
    }

    match index_type {
        0 => LowCardinalityColumn::<u8>::load_column(reader, rows, keys).await,
        1 => LowCardinalityColumn::<u16>::load_column(reader, rows, keys).await,
        2 => LowCardinalityColumn::<u32>::load_column(reader, rows, keys).await,
        3 => LowCardinalityColumn::<u64>::load_column(reader, rows, keys).await,
        _ => err!(DriverError::BrokenData),
    }
}

async fn load_column<R>(mut reader: R, field: &Field, rows: u64) -> Result<Box<dyn AsInColumn>>
where
    R: AsyncBufRead + Unpin,
{
    let nulls = if field.is_nullable() {
        Some(load_nulls(reader.borrow_mut(), rows).await?)
    } else {
        None
    };
    // SqlType-to-Loader dispatch implementation
    // TODO: rewrite as Fabric method
    let col: Box<dyn AsInColumn> = match field.sql_type {
        SqlType::String => StringColumn::load_string_column(reader, rows)
            .await?
            .set_nulls(nulls),
        SqlType::FixedString(width) => StringColumn::load_fixed_string_column(reader, rows, width)
            .await?
            .set_nulls(nulls),
        SqlType::UInt64 => FixedColumn::<u64>::load_column(reader, rows)
            .await?
            .set_nulls(nulls),
        SqlType::Int64 => FixedColumn::<u64>::load_column(reader, rows)
            .await?
            .cast::<i64>()
            .set_nulls(nulls),
        SqlType::UInt32 => FixedColumn::<u32>::load_column(reader, rows)
            .await?
            .set_nulls(nulls),
        SqlType::Int32 => FixedColumn::<u32>::load_column(reader, rows)
            .await?
            .cast::<i32>()
            .set_nulls(nulls),
        SqlType::UInt16 => FixedColumn::<u16>::load_column(reader, rows)
            .await?
            .set_nulls(nulls),
        SqlType::Int16 => FixedColumn::<u16>::load_column(reader, rows)
            .await?
            .cast::<u16>()
            .set_nulls(nulls),
        SqlType::UInt8 => FixedColumn::<u8>::load_column(reader, rows)
            .await?
            .set_nulls(nulls),
        SqlType::Int8 => FixedColumn::<u8>::load_column(reader, rows)
            .await?
            .cast::<u8>()
            .set_nulls(nulls),
        SqlType::Float32 => {
            FixedColumn::<u32>::load_column(reader, rows)
                .await?
                .cast::<f32>()
                .set_nulls(nulls)
            //FixedColumn::<f32>::load_column(reader, rows).await?
        }
        SqlType::Float64 => FixedColumn::<u64>::load_column(reader, rows)
            .await?
            .cast::<f64>()
            .set_nulls(nulls),
        SqlType::Enum8 => EnumColumn::<i8>::load_column(reader, rows, field)
            .await?
            .set_nulls(nulls),
        SqlType::Enum16 => EnumColumn::<i16>::load_column(reader, rows, field)
            .await?
            .set_nulls(nulls),
        SqlType::Date => FixedColumn::<u16>::load_column(reader, rows)
            .await?
            .cast::<ValueDate>()
            .set_nulls(nulls),
        SqlType::DateTime64(_p, _) => FixedColumn::<u64>::load_column(reader, rows)
            .await?
            .cast::<ValueDateTime64>()
            .set_nulls(nulls),
        SqlType::DateTime => FixedColumn::<ValueDateTime>::load_column(reader, rows)
            .await?
            .set_nulls(nulls),
        SqlType::Uuid => FixedColumn::<u128>::load_column(reader, rows)
            .await?
            .cast::<ValueUuid>()
            .set_nulls(nulls),
        SqlType::Ipv4 => FixedColumn::<u32>::load_column(reader, rows)
            .await?
            .cast::<ValueIp4>()
            .set_nulls(nulls),
        SqlType::Ipv6 => FixedColumn::<u128>::load_column(reader, rows)
            .await?
            .cast::<ValueIp6>()
            .set_nulls(nulls),
        SqlType::Decimal(p, _s) => match p {
            v if i32::fit(v) => FixedColumn::<u32>::load_column(reader, rows)
                .await?
                .cast::<ValueDecimal32>()
                .set_nulls(nulls),
            v if i64::fit(v) => FixedColumn::<u64>::load_column(reader, rows)
                .await?
                .cast::<ValueDecimal64>()
                .set_nulls(nulls),
            #[cfg(feature = "int128")]
            v if i128::fit(v) => FixedColumn::<u128>::load_column(reader, rows)
                .await?
                .cast::<ValueDecimal128>()
                .set_nulls(nulls),
            _ => return err!(DriverError::UnsupportedType(field.sql_type)),
        },
        _ => {
            return err!(DriverError::UnsupportedType(field.sql_type));
        }
    };

    Ok(col)
}

async fn read_block<R>(
    mut reader: R,
    columns: &[BlockColumnHeader],
    timezone: Tz,
) -> Result<Option<ServerBlock>>
where
    R: AsyncBufRead + Unpin,
{
    // Read block-info (BI)
    let block_info = read_block_info(reader.borrow_mut()).await?;
    // Shortcut parse time for empty block but return header-only to caller
    if block_info.is_empty() {
        return Ok(None);
    }

    let mut block_columns = Vec::new();

    debug_assert!(columns.is_empty() || columns.len() == block_info.cols as usize);

    let mut l: u64;
    // Read block-data (BLK)
    for col_idx in 0..block_info.cols {
        let mut rdr = ValueReader::new(reader.borrow_mut());
        // Read FNM
        l = rdr.read_vint().await?;
        let name: String = rdr.read_string(l).await?;
        // Read TNM
        l = rdr.read_vint().await?;
        let field: String = rdr.read_string(l).await?;
        // TODO. Verify is every query block has the same order of columns?
        let field = {
            if columns.is_empty() {
                parse_type_field(field.as_str())?
            } else {
                // We strongly rely on consistence of column order in blocks
                // All sequential blocks of the query must have the same field order
                unsafe { columns.get_unchecked(col_idx as usize).field.clone() }
            }
        };

        if field.is_array() {
            return err!(DriverError::UnsupportedType(SqlType::Array));
        } else if field.is_lowcardinality() && field.sql_type != SqlType::String {
            return err!(DriverError::UnsupportedType(SqlType::LowCardinality));
        }
        // Read DATA
        let col = if block_info.rows == 0 {
            Box::new(()) as Box<dyn AsInColumn>
        } else if field.is_lowcardinality() {
            load_lowcardinality_string(reader.borrow_mut(), &field, block_info.rows).await?
        } else {
            load_column(reader.borrow_mut(), &field, block_info.rows).await?
        };
        // Append column to the list of block column adapters (dyn AsInColunm interface)
        block_columns.push(BlockColumn {
            data: col,
            header: BlockColumnHeader { field, name },
        });
    }

    Ok(Some(ServerBlock {
        columns: block_columns,
        rows: block_info.rows,
        timezone,
    }))
}

async fn read_exception<R>(reader: R) -> Result<Box<[Exception]>>
where
    R: AsyncBufRead + Unpin,
{
    let mut resp = Vec::with_capacity(1);
    let mut rdr = ValueReader::new(reader);
    let mut code = [0u8; 4];
    let mut l: u64;

    loop {
        rdr.as_mut().read_exact(&mut code[..]).await?;

        l = rdr.read_vint().await?;
        let name = rdr.read_string(l).await?;
        l = rdr.read_vint().await?;
        let message = rdr.read_string(l).await?;
        l = rdr.read_vint().await?;
        let _trace: String = rdr.read_string(l).await?;

        let nested = rdr.read_byte().await?;

        let one = Exception {
            code: u32::from_le_bytes(code),
            name,
            message,
            #[cfg(test)]
            trace: _trace,
        };

        //#[cfg(not(test))]
        //    drop(trace);

        resp.push(one);
        if nested == 0u8 {
            return Ok(resp.into_boxed_slice());
        }
    }
}

async fn read_hello<R>(reader: R) -> Result<Response>
where
    R: AsyncBufRead + Unpin,
{
    let mut rdr = ValueReader::new(reader);
    let l = rdr.read_vint().await?;
    let name: String = rdr.read_string(l).await?;
    let major = rdr.read_vint().await?;
    let minor = rdr.read_vint().await?;
    let revision = rdr.read_vint().await?;

    let timezone: Tz = {
        if revision as u32 >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
            let l = rdr.read_vint().await?;
            let timezone: String = rdr.read_string(l).await?;
            timezone.parse().unwrap_or(Tz::UTC)
        } else {
            Tz::UTC
        }
    };

    if revision as u32 >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
        let l = rdr.read_vint().await?;
        rdr.skip(l).await?;
    }

    if revision as u32 >= DBMS_MIN_REVISION_WITH_VERSION_PATCH {
        let _patch: u64 = rdr.read_vint().await?;
    }

    Ok(Response::Hello(name, major, minor, revision, timezone))
}

async fn read_progress<R>(reader: R, revision: u32) -> Result<(u64, u64, u64)>
where
    R: AsyncBufRead + Unpin,
{
    let mut rdr = ValueReader::new(reader);

    let rows: u64 = rdr.read_vint().await?;
    let bytes: u64 = rdr.read_vint().await?;

    let total: u64 = if revision >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS {
        rdr.read_vint().await? //skip this value
    } else {
        0
    };
    Ok((rows, bytes, total))
}

async fn read_profile<R>(reader: R) -> Result<ProfileInfo>
where
    R: AsyncBufRead + Unpin,
{
    let mut rdr = ValueReader::new(reader);

    let rows: u64 = rdr.read_vint().await?;
    let blocks: u64 = rdr.read_vint().await?;
    let bytes: u64 = rdr.read_vint().await?;
    let applied_limit = rdr.read_byte().await?;
    let rows_before_limit: u64 = rdr.read_vint().await?;
    let calculated_rows_before_limit = rdr.read_byte().await?;

    Ok(ProfileInfo {
        rows,
        blocks,
        bytes,
        applied_limit,
        rows_before_limit,
        calculated_rows_before_limit,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::errors::Result;
    use crate::protocol::encoder::Encoder;
    use std::cmp;
    use std::io;
    use std::mem::size_of_val;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::AsyncRead;

    struct AsyncChunk<'a> {
        buf: &'a [u8],
        cs: u8,
        break_: bool,
        break_cnt: u8,
    }

    impl<'a> AsyncChunk<'a> {
        fn new(chunk_size: u8, make_break: bool, buf: &'a [u8]) -> AsyncChunk<'a> {
            AsyncChunk {
                buf,
                cs: chunk_size,
                break_: make_break,
                break_cnt: 1,
            }
        }
    }

    impl<'a> AsyncRead for AsyncChunk<'a> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            let size = cmp::min(self.cs as usize, buf.len());
            if size == 0 {
                return Poll::Ready(Ok(0));
            };

            let me = self.get_mut();

            if me.break_ && me.break_cnt > 0 {
                me.break_cnt -= 1;
                if me.break_cnt == 0 {
                    cx.waker().wake_by_ref();
                    me.break_cnt = 2;
                    return Poll::Pending;
                }
            }
            let size = io::Read::read(&mut me.buf, &mut buf[0..size])?;
            Ok(size).into()
        }
    }

    impl<'a> AsyncBufRead for AsyncChunk<'a> {
        fn poll_fill_buf(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
            // Force AsyncBufRead to read by small chunks
            Poll::Ready(Ok(&[]))
        }

        fn consume(self: Pin<&mut Self>, _amt: usize) {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_async_int() -> Result<()> {
        for v in vec![0, 10, 64, 1000, 400000, 50000000].into_iter() {
            let mut buf = Vec::with_capacity(20);
            v.encode(&mut buf)?;
            let istr = AsyncChunk::new(2, true, buf.as_ref());
            let mut istr = ValueReader::new(istr);
            let v1 = istr.read_vint().await?;
            assert_eq!(v, v1);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_async_bool() -> Result<()> {
        let buf = vec![0, 1, 0, 1];
        let istr = AsyncChunk::new(2, true, buf.as_ref());
        let mut istr = ValueReader::new(istr);
        for i in 0..4 {
            let v = istr.read_byte().await?;
            assert_eq!(v, i % 2);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_async_string() -> Result<()> {
        let v = "test_async_string".to_string();
        let mut buf = Vec::new();
        v.encode(&mut buf)?;

        println!("marshaled {:x?}", &buf[..]);
        let istr = AsyncChunk::new(6, true, buf.as_ref());
        let mut istr = ValueReader::new(istr);

        let fut = istr.read_vint();
        println!("size of  readVint  {}", size_of_val(&fut));
        let l = fut.await?;

        let fut = istr.read_string(l);
        println!("size of  readString  {}", size_of_val(&fut));
        let v1: String = fut.await?;
        assert_eq!(v, v1);

        Ok(())
    }
}

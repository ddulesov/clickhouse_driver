use futures::TryFutureExt;
use std::marker::Unpin;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use super::block::{Block, BlockColumnHeader, EmptyBlock, OutputBlockWrapper, ServerBlock};
use super::command::{CommandSink, ResponseStream};
use crate::errors::{ConversionError, DriverError, Result};

use super::ServerWriter;
use crate::protocol::column::EnumIndex;

const DEFAULT_INSERT_BUFFER_SIZE: usize = 8 * 1024;

pub struct InsertSink<'a, R: AsyncRead, W> {
    pub(crate) inner: ResponseStream<'a, R>,
    pub(crate) sink: CommandSink<W>,
    pub(crate) buf: Vec<u8>,
    pub(crate) timeout: Duration,
    #[allow(dead_code)]
    pub(crate) columns: Vec<BlockColumnHeader>,
}

impl<'a, R: AsyncRead, W> Drop for InsertSink<'a, R, W> {
    fn drop(&mut self) {
        self.inner.clear_pending()
    }
}

impl<'a, R: AsyncRead + Unpin, W: AsyncWrite + Unpin> InsertSink<'a, R, W> {
    pub(crate) fn new(
        tcpstream: ResponseStream<'a, R>,
        sink: CommandSink<W>,
        block: ServerBlock,
        timeout: Duration,
    ) -> InsertSink<'a, R, W> {
        let buf = Vec::with_capacity(DEFAULT_INSERT_BUFFER_SIZE);

        let mut columns = block.into_headers();
        // prepare Enum8 and Enum16  for string->data conversion by sorting index by string
        for column in columns.iter_mut() {
            if let Some(meta) = column.field.get_meta_mut() {
                meta.index.sort_unstable_by(EnumIndex::fn_sort_str);
            }
        }

        InsertSink {
            inner: tcpstream,
            sink,
            buf,
            timeout,
            columns,
        }
    }
    /// Send block of data to Clickhouse server
    pub async fn next(&mut self, data: Block<'_>) -> Result<()> {
        self.buf.clear();

        if data.column_count() != self.columns.len() {
            return Err(DriverError::BrokenData.into());
        }
        // TODO split huge block on chunks less then MAX_BLOCK_SIZE size each
        // Now the caller responsible to split data
        if data.row_count() > crate::MAX_BLOCK_SIZE {
            return Err(DriverError::BrokenData.into());
        }
        // for efficiency check input data and column data format compatibility only once
        if !self
            .columns
            .iter()
            .zip(data.column_iter())
            .all(|(head, col)| 
                head.name.eq(col.name) && head.field.nullable == col.nullable && col.data.is_compatible(&head.field)
            )
        {
            return Err(ConversionError::UnsupportedConversion.into());
        }

        // TODO get rid of intermediate buffer. Write block right into stream
        OutputBlockWrapper {
            columns: &self.columns,
            inner: data,
        }
        .write(self.inner.info_ref(), &mut self.buf)?;

        self.sink
            .writer
            .write_all(self.buf.as_ref())
            .map_err(Into::into)
            .await
    }

    pub async fn commit(&mut self) -> Result<()> {
        self.buf.clear();
        EmptyBlock.write(self.inner.info_ref(), &mut self.buf)?;
        self.sink.writer.write_all(self.buf.as_ref()).await?;

        if let Some(packet) = self.inner.next(self.timeout).await? {
            return Err(DriverError::PacketOutOfOrder(packet.code()).into());
        }

        //self.inner.set_fuse();
        Ok(())
    }
}

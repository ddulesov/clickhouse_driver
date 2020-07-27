use std::fmt;
use std::io;
use std::net::Shutdown;
use std::time::Duration;

use crate::pool::Inner as InnerPool;
use crate::protocol::block::{Block, ServerBlock};
use crate::protocol::command::{CommandSink, ResponseStream};
use crate::protocol::insert::InsertSink;
use crate::protocol::packet::Response;
use crate::protocol::packet::{Command, Execute, Hello, Ping};
use crate::protocol::query::Query;
use crate::protocol::{CompressionMethod, ServerWriter};
use crate::{
    errors::{DriverError, Result},
    pool::{options::Options, Pool},
};
use crate::{FLAG_DETERIORATED, FLAG_PENDING};
use chrono_tz::Tz;
use log::info;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;

const DEF_OUT_BUF_SIZE: usize = 512;

#[derive(Debug)]
pub(super) struct ServerInfo {
    pub(crate) revision: u32,
    pub(crate) readonly: u8,
    pub(crate) flag: u8,
    pub(crate) compression: CompressionMethod,
    pub(crate) timezone: Tz,
}

impl ServerInfo {
    #[inline]
    pub(crate) fn set_deteriorated(&mut self) {
        self.flag |= FLAG_DETERIORATED;
    }
    #[inline]
    pub(crate) fn set_pending(&mut self) {
        self.flag |= FLAG_PENDING;
    }
    #[inline]
    pub(crate) fn clear_pending(&mut self) {
        self.flag &= !FLAG_PENDING;
    }

    #[inline]
    pub(crate) fn is_pending(&self) -> bool {
        (self.flag & FLAG_PENDING) == FLAG_PENDING
    }
}

pub(super) struct Inner {
    pub(crate) socket: Option<TcpStream>,
    pub(crate) info: ServerInfo,
}

pub(super) type InnerConection = Inner;

pub struct QueryResult<'a, R: AsyncRead, W: AsyncWrite> {
    timeout: Duration,
    pub(crate) inner: ResponseStream<'a, R>,
    sink: CommandSink<W>,
}

impl<'a, R: AsyncRead + Unpin, W: AsyncWrite + Unpin> QueryResult<'a, R, W> {
    pub async fn next(&mut self) -> Result<Option<ServerBlock>> {
        while let Some(packet) = self.inner.next(self.timeout).await? {
            if let Response::Data(block) = packet {
                return Ok(Some(block));
            } else {
                //println!("packet {:?}", packet);
            }
        }
        Ok(None)
    }

    #[inline]
    pub fn is_pending(&self) -> bool {
        self.inner.is_pending()
    }

    #[inline]
    pub async fn cancel(&mut self) {
        let _r = self.sink.cancel().await;
    }
}

impl<'a, R: AsyncRead, W: AsyncWrite> Drop for QueryResult<'a, R, W> {
    fn drop(&mut self) {}
}

impl Default for Inner {
    fn default() -> Inner {
        Inner {
            socket: None,
            info: ServerInfo {
                flag: 0,
                revision: crate::CLICK_HOUSE_REVISION as u32,
                timezone: chrono_tz::UTC,
                compression: CompressionMethod::None,
                readonly: 0,
            },
        }
    }
}

impl ServerContext for Inner {
    #[inline]
    fn info(&self) -> &ServerInfo {
        &self.info
    }
}

impl Inner {
    #[inline]
    /// Is connection not initialized yet
    fn is_null(&self) -> bool {
        self.socket.is_none()
    }
    /// Check is the socket connected to peer
    pub(super) fn is_ok(&self) -> bool {
        self.socket
            .as_ref()
            .map_or(false, |socket| socket.peer_addr().is_ok())
    }

    /// Split self into Tcp read-half, Tcp write-half, and ServerInfo
    pub(super) fn split(&mut self) -> Option<(ReadHalf<'_>, WriteHalf<'_>, &mut ServerInfo)> {
        let info = &mut self.info as *mut ServerInfo;
        // SAFETY: This can be risky if caller use returned values inside Connection
        // or InnerConnection methods. Nothing do it.
        match self.socket {
            None => None,
            Some(ref mut socket) => unsafe {
                let (rdr, wrt) = socket.split();
                Some((rdr, wrt, &mut *info))
            },
        }
    }

    /// Establish connection to the server  
    pub(super) async fn init(inner_pool: &InnerPool, addr: &str) -> Result<Box<Inner>> {
        let socket = TcpStream::connect(addr).await?;
        Inner::setup_stream(&socket, &inner_pool.options)?;
        info!(
            "connection  established to: {}",
            socket.peer_addr().unwrap()
        );

        let conn = Inner::handshake(socket, &inner_pool.options).await?;
        Ok(conn)
    }

    pub async fn handshake(mut socket: TcpStream, options: &Options) -> Result<Box<Self>> {
        info!("handshake complete");

        let mut inner: Box<Inner> = Box::new(Default::default());
        {
            let mut buf = Vec::with_capacity(256);
            {
                Hello { opt: options }.write(&inner.info, &mut buf)?;
            }

            let (rdr, wrt) = socket.split();
            let mut sink = CommandSink::new(wrt);
            sink.writer.write_all(&buf[..]).await?;
            drop(buf);
            let mut stream = ResponseStream::with_capacity(256, rdr, &mut inner.info);

            match stream.next(options.connection_timeout).await? {
                Some(Response::Hello(_name, _major, _minor, revision, tz)) => {
                    inner.info.revision = revision as u32;
                    inner.info.timezone = tz;
                    inner.socket = Some(socket);
                }
                _ => {
                    socket.shutdown(Shutdown::Both)?;
                    return Err(DriverError::ConnectionTimeout.into());
                }
            };
            inner.info.compression = options.compression;
        }
        Ok(inner)
    }

    #[inline]
    fn setup_stream(socket: &TcpStream, options: &Options) -> io::Result<()> {
        socket.set_keepalive(options.keepalive)?;
        socket.set_nodelay(true)
    }

    async fn cleanup(&mut self) -> Result<()> {
        //TODO add any finalization stuff
        if (self.info.flag & FLAG_PENDING) == FLAG_PENDING {
            let wrt = if let Some((_, wrt, _info)) = self.split() {
                wrt
            } else {
                return Err(DriverError::ConnectionClosed.into());
            };

            CommandSink::new(wrt).cancel().await?;
        };

        Ok(())
    }
}

pub(crate) trait ServerContext {
    fn info(&self) -> &ServerInfo;
}

pub struct Connection {
    inner: Box<Inner>,
    pub(crate) pool: Option<Pool>,
    out: Vec<u8>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let peer = self.inner.socket.as_ref().map_or("-".to_string(), |s| {
            s.peer_addr().map_or("-".to_string(), |p| format!("{}", p))
        });
        f.debug_struct("Connection")
            .field("inner.pear", &peer)
            .finish()
    }
}

macro_rules! check_pending {
    ($this:ident) => {
        if $this.inner.info.is_pending() {
            return Err(DriverError::OperationInProgress.into());
        }
    };
}

impl Connection {
    /// Assemble connection from socket(Inner) and pool object
    pub(super) fn new(pool: Pool, mut conn: Box<Inner>) -> Connection {
        conn.info.flag = 0;
        Connection {
            pool: Some(pool),
            inner: conn,
            out: Vec::with_capacity(512),
        }
    }

    /// Returns Clickhouse server revision
    #[inline]
    pub fn revision(&self) -> u32 {
        self.inner.info.revision
    }

    /// Check if a connection has pending status.
    /// connection get pending status on query call until all data will be fetched
    /// or in insert call until commit
    #[inline]
    pub fn is_pending(&self) -> bool {
        self.inner.info.is_pending()
    }

    /// Returns Clickhouse server timezone
    #[inline]
    pub fn timezone(&self) -> Tz {
        self.inner.info.timezone
    }

    /// Mark connection as drain. This will recycle it on drop instead of retuning to pool
    #[inline]
    pub(super) fn set_deteriorated(&mut self) {
        self.inner.info.flag |= FLAG_DETERIORATED;
    }

    /// Disconnects this connection from server.
    pub(super) fn disconnect(mut self) -> Result<()> {
        if let Some(socket) = self.inner.socket.take() {
            socket.shutdown(Shutdown::Both)?;
        }
        Ok(())
    }
    /// Get pool specific options or  default ones if former is not specified
    fn options(&self) -> &Options {
        self.pool.as_ref().map_or_else(
            || {
                let o: &Options = &*crate::DEF_OPTIONS;
                o
            },
            |p| &p.inner.options,
        )
    }

    /// Perform cleanum  and disconnect from server
    pub async fn close(mut self) -> Result<()> {
        self.inner.cleanup().await?;
        self.disconnect()
    }

    /// Ping-pong connection verification
    pub async fn ping(&mut self) -> Result<()> {
        let timeout = self.options().ping_timeout;

        let (mut stream, mut sink, buf) = self.write_command(&Ping)?;
        sink.writer.write_all(buf.as_slice()).await?;
        buf.truncate(DEF_OUT_BUF_SIZE);

        while let Some(packet) = stream.next(timeout).await? {
            match packet {
                Response::Pong => {
                    return Ok(());
                }
                _ => {
                    continue;
                    // return Err(DriverError::PacketOutOfOrder(packet.code()).into());
                }
            }
        }
        Err(DriverError::ConnectionTimeout.into())
    }

    /// Execute DDL query
    pub async fn execute(&mut self, ddl: impl Into<Query>) -> Result<()> {
        check_pending!(self);
        let timeout = self.options().execute_timeout;

        let query: Query = ddl.into();
        let (mut stream, mut sink, buf) = self.write_command(&Execute { query })?;
        sink.writer.write_all(buf).await?;

        if let Some(packet) = stream.next(timeout).await? {
            return Err(DriverError::PacketOutOfOrder(packet.code()).into());
        }

        Ok(())
    }

    /// Execute INSERT query sending Block of data
    /// Returns InsertSink that can be used to streaming next Blocks of data
    pub async fn insert(
        &mut self,
        data: &Block<'_>,
    ) -> Result<InsertSink<'_, ReadHalf<'_>, WriteHalf<'_>>> {
        check_pending!(self);

        let query = Query::from_block(&data);
        let timeout = self.options().execute_timeout;

        let (mut stream, mut sink, buf) = self.write_command(&Execute { query })?;
        sink.writer.write_all(buf.as_slice()).await?;
        buf.truncate(DEF_OUT_BUF_SIZE);

        // We get first block that has not rows. Columns define table structure
        // Before call insert we will check input data against server table structure
        stream.skip_empty = false;
        stream.set_pending();
        let mut stream = if let Some(Response::Data(block)) = stream.next(timeout).await? {
            InsertSink::new(stream, sink, block, timeout)
        } else {
            stream.set_deteriorated();
            return Err(DriverError::PacketOutOfOrder(0).into());
        };

        stream.next(&data).await?;
        Ok(stream)
    }
    /// Execute SELECT query returning Block of data
    pub async fn query(
        &mut self,
        sql: impl Into<Query>,
    ) -> Result<QueryResult<'_, ReadHalf<'_>, WriteHalf<'_>>> {
        check_pending!(self);

        let timeout = self.options().query_timeout;
        let (stream, mut sink, buf) = self.write_command(&Execute { query: sql.into() })?;
        sink.writer.write_all(buf.as_slice()).await?;
        buf.truncate(DEF_OUT_BUF_SIZE);

        Ok(QueryResult {
            sink,
            inner: stream,
            timeout,
        })
    }

    /// Take inner connection. Drain itself
    pub(super) fn take(&mut self) -> Box<Inner> {
        std::mem::replace(&mut self.inner, Box::new(Inner::default()))
    }

    fn write_command(
        &mut self,
        cmd: &dyn Command,
    ) -> Result<(
        ResponseStream<'_, ReadHalf<'_>>,
        CommandSink<WriteHalf<'_>>,
        &mut Vec<u8>, //&[u8],
    )> {
        //todo!("truncate output buffer upon completion ");
        self.out.clear();

        let (rdr, wrt, info) = if let Some((rdr, wrt, info)) = self.inner.split() {
            (rdr, wrt, info)
        } else {
            return Err(DriverError::ConnectionClosed.into());
        };

        cmd.write(&*info, &mut self.out)?;
        info.set_pending();

        Ok((
            ResponseStream::new(rdr, info),
            CommandSink::new(wrt),
            self.out.as_mut(), //self.out.as_ref(),
        ))
    }
}

pub(crate) fn disconnect(mut conn: Box<Inner>) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move { conn.cleanup().await });
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }

        let conn = self.take();
        if conn.is_null() {
            return;
        }
        if let Some(pool) = self.pool.take() {
            pool.return_conn(conn);
        } else {
            disconnect(conn);
        }
    }
}

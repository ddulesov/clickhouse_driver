use std::convert::TryInto;
use std::{
    fmt,
    sync::atomic::{self, Ordering},
    sync::Arc,
};

pub use builder::PoolBuilder;
use crossbeam::queue;
pub use options::Options;
use tokio::sync;
use tokio::time::delay_for;
use util::*;

use crate::{
    client::{disconnect, Connection, InnerConection},
    errors::{Result, UrlError},
};

use self::disconnect::DisconnectPool;

pub mod builder;
mod disconnect;
pub mod options;

#[cfg(feature = "recycle")]
mod recycler;
mod util;

/// Normal operation status
const POOL_STATUS_SERVE: i8 = 0;
/// Pool in progress of shutting down.
/// New connection request will be rejected
const POOL_STATUS_STOPPING: i8 = 1;
/// Pool is stopped.
/// All  connections are freed
#[allow(dead_code)]
const POOL_STATUS_STOPPED: i8 = 2;

/// Pool implementation (inner sync structure)
pub(crate) struct Inner {
    new: queue::ArrayQueue<Box<InnerConection>>,
    /// The number of issued connections
    /// This value is in range of 0 .. options.max_pool
    issued: atomic::AtomicUsize,
    /// Pool options
    pub(crate) options: Options,
    /// Used for notification of tasks which wait for available connection
    notifyer: sync::Notify,
    #[cfg(feature = "recycle")]
    recycler: Option<sync::mpsc::UnboundedReceiver<Option<Box<InnerConection>>>>,
    /// Server host addresses
    hosts: Vec<String>,
    /// The number of active connections that is taken by tasks
    connections_num: atomic::AtomicUsize,
    /// Number of tasks in waiting queue
    wait: atomic::AtomicUsize,
    /// Pool status flag
    close: atomic::AtomicI8,
    //disconnect: Option<Waker>,

    // min: u16,
    // max: u16,
}

impl Inner {
    #[cfg(not(feature = "recycle"))]
    #[inline]
    fn spawn_recycler(self: &mut Arc<Inner>) {}

    #[cfg(feature = "recycle")]
    fn spawn_recycler(self: &mut Arc<Inner>) {
        use recycler::Recycler;

        let dropper = if let Some(inner) = Arc::get_mut(self) {
            inner.recycler.take()
        } else {
            None
        };

        //use ttl_check_inerval::TtlCheckInterval;
        if let Some(dropper) = dropper {
            // Spawn the Recycler.
            tokio::spawn(Recycler::new(Arc::clone(self), dropper));
        }
    }

    fn closed(&self) -> bool {
        self.close.load(atomic::Ordering::Acquire) > POOL_STATUS_SERVE
    }
}

/// Asynchronous pool of Clickhouse connections.
#[derive(Clone)]
pub struct Pool {
    pub(crate) inner: Arc<Inner>,
    #[cfg(feature = "recycle")]
    drop: sync::mpsc::UnboundedSender<Option<Box<InnerConection>>>,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("inner.min", &self.inner.options.pool_min)
            .field("inner.max", &self.inner.options.pool_max)
            .field("queue len", &self.inner.new.len())
            .field("issued", &self.inner.issued)
            .finish()
    }
}

impl Pool {
    #[inline]
    pub fn create<T>(options: T) -> Result<Pool>
    where
        T: TryInto<Options, Error = UrlError>,
    {
        let options = options.try_into()?;
        PoolBuilder::create(options)
    }

    #[inline(always)]
    pub fn info(&self) -> PoolInfo {
        let inner = &self.inner;

        util::PoolInfo {
            idle: inner.new.len(),
            issued: inner.issued.load(Ordering::Relaxed),
            wait: inner.wait.load(Ordering::Relaxed),
        }
    }

    /// Request new connection. Return connection from poll or create new if
    /// poll doesn't have idle connections
    pub async fn connection(&self) -> Result<Connection> {
        let inner = &self.inner;
        loop {
            if inner.closed() {
                return Err(crate::errors::DriverError::PoolDisconnected.into());
            }
            if let Ok(conn) = inner.new.pop() {
                let mut conn = Connection::new(self.clone(), conn);

                if inner.options.ping_before_query {
                    let mut c = inner.options.send_retries;
                    loop {
                        if conn.ping().await.is_ok() {
                            return Ok(conn);
                        }

                        if c <= 1 {
                            break;
                        }

                        delay_for(inner.options.retry_timeout).await;
                        c -= 1;
                    }

                    conn.set_deteriorated();
                    self.return_connection(conn);
                    continue;
                } else {
                    return Ok(conn);
                }
            };

            let num = inner.issued.load(Ordering::Relaxed);

            if num >= inner.options.pool_max as usize {
                inner.wait.fetch_add(1, Ordering::AcqRel);
                inner.notifyer.notified().await;
                inner.wait.fetch_sub(1, Ordering::AcqRel);
            } else {
                let num = inner.issued.fetch_add(1, Ordering::AcqRel);
                if num < inner.options.pool_max as usize {
                    break;
                } else {
                    inner.issued.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }

        for addr in self.get_addr_iter() {
            match InnerConection::init(inner, addr).await {
                Ok(conn) => {
                    return Ok(Connection::new(self.clone(), conn));
                }
                Err(err) => {
                    if !err.is_timeout() {
                        return Err(err);
                    }
                }
            }
        }

        //release quota
        inner.issued.fetch_sub(1, Ordering::AcqRel);
        self.inner.notifyer.notify();
        Err(crate::errors::DriverError::ConnectionClosed.into())
    }

    /// Return connection  to pool
    pub fn return_connection(&self, mut conn: Connection) {
        // NOTE: It's  safe to call it out of tokio runtime
        let pool = conn.pool.take();
        debug_assert_eq!(pool.as_ref().unwrap(), self);

        self.return_conn(conn.take());
    }

    /// Take back connection to pool if connection is not deterogated
    /// and pool is not full . Recycle in other case.
    pub(crate) fn return_conn(&self, conn: Box<InnerConection>) {
        // NOTE: It's  safe to call it out of tokio runtime
        let conn = if conn.is_ok()
            && !conn.info.flag == 0
            && !self.inner.closed()
            && self.inner.new.len() < self.inner.options.pool_min as usize
        {
            match self.inner.new.push(conn) {
                Ok(_) => {
                    self.inner.notifyer.notify();
                    return;
                }
                Err(econn) => econn.0,
            }
        } else {
            conn
        };

        self.inner.issued.fetch_sub(1, Ordering::AcqRel);
        self.inner.notifyer.notify();
        self.send_to_recycler(conn);
    }

    #[cfg(not(feature = "recycle"))]
    #[inline]
    fn send_to_recycler(&self, conn: Box<InnerConection>) {
        disconnect(conn);
    }

    #[cfg(feature = "recycle")]
    #[inline]
    fn send_to_recycler(&self, conn: Box<InnerConection>) {
        if let Err(conn) = self.drop.send(Some(conn)) {
            let conn = conn.0.unwrap();
            // This _probably_ means that the Runtime is shutting down, and that the Recycler was
            // dropped rather than allowed to exit cleanly.
            if self.inner.close.load(atomic::Ordering::SeqCst) != POOL_STATUS_STOPPED {
                // Yup, Recycler was forcibly dropped!
                // All we can do here is try the non-pool drop path for Conn.
                drop(conn);
            } else {
                unreachable!("Recycler exited while connections still exist");
            }
        }
    }

    #[cfg(feature = "recycle")]
    pub fn disconnect(self) -> DisconnectPool {
        let _ = self.drop.send(None).is_ok();
        DisconnectPool::new(self)
    }

    #[cfg(not(feature = "recycle"))]
    pub fn disconnect(self) -> DisconnectPool {
        DisconnectPool::new(self)
    }

    fn get_addr_iter(&'_ self) -> AddrIter<'_> {
        let inner = &self.inner;
        let index = if inner.hosts.len() > 1 {
            inner.connections_num.fetch_add(1, Ordering::Relaxed)
        } else {
            inner.connections_num.load(Ordering::Relaxed)
        };
        util::AddrIter::new(inner.hosts.as_slice(), index, self.options().send_retries)
    }

    #[inline]
    pub fn options(&self) -> &Options {
        &self.inner.options
    }
}

impl PartialEq for Pool {
    fn eq(&self, other: &Pool) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

#[cfg(test)]
mod test {
    use super::builder::PoolBuilder;
    use super::Result;

    #[tokio::test]
    async fn test_build_pool() -> Result<()> {
        let pool = PoolBuilder::default()
            .with_database("default")
            .with_username("default")
            .add_addr("www.yandex.ru:9000")
            .build()
            .unwrap();

        assert_eq!(pool.options().username, "default");
        assert_eq!(pool.inner.hosts[0], "www.yandex.ru:9000");

        Ok(())
    }
}

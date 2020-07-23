use clickhouse_driver::prelude::errors;
use clickhouse_driver::prelude::*;
use std::env;
use std::io;
use std::time::Duration;
use tokio::{self, time::delay_for};

// macro_rules! check {
//     ($f:expr) => {
//         match $f {
//             Ok(r) => r,
//             Err(err) => {
//                 eprintln!("{:?}", err);
//                 Default::default()
//             }
//         }
//     };
// }

pub fn get_pool() -> Pool {
    let database_url =
        env::var("DATABASE_URL")
            .unwrap_or_else(|_| "tcp://localhost?execute_timeout=5s&query_timeout=20s&pool_max=4&compression=lz4".into());

    Pool::create(database_url).expect("provide connection url in DATABASE_URL env variable")
}

#[tokio::test]
async fn test_connection_pool() -> io::Result<()> {
    let pool = get_pool();

    let mut h: Vec<_> = (0..10)
        .map(|_| {
            let pool = pool.clone();

            tokio::spawn(async move {
                let mut conn = pool.connection().await.unwrap();
                conn.ping().await.expect("ping ok");
                delay_for(Duration::new(2, 0)).await;
            })
        })
        .collect();

    for (_, hnd) in h.iter_mut().enumerate() {
        hnd.await?;
    }

    Ok(())
}

#[tokio::test]
async fn test_ping() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;
    conn.ping().await?;
    Ok(())
}

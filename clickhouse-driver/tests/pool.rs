use clickhouse_driver::prelude::errors;
use clickhouse_driver::prelude::*;
use std::io;
use std::time::Duration;
use tokio::{self, time::sleep};

mod common;
use common::{get_config, get_pool, get_pool_extend};

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

#[tokio::test]
async fn test_connection_pool() -> io::Result<()> {
    let pool = get_pool();

    let mut h: Vec<_> = (0..10)
        .map(|_| {
            let pool = pool.clone();

            tokio::spawn(async move {
                let mut conn = pool.connection().await.unwrap();
                conn.ping().await.expect("ping ok");
                sleep(Duration::new(2, 0)).await;
            })
        })
        .collect();

    for (_, hnd) in h.iter_mut().enumerate() {
        hnd.await?;
    }

    Ok(())
}

#[tokio::test]
async fn test_conn_race() -> io::Result<()> {
    let pool = get_pool_extend("tcp://localhost/?pool_max=10&pool_min=10&ping_before_query=false");
    let now = std::time::Instant::now();
    const COUNT: u32 = 1000;
    const MSEC_100: u32 = 100000000;

    let mut h: Vec<_> = (0..COUNT)
        .map(|_| {
            let pool = pool.clone();

            tokio::spawn(async move {
                let _ = pool.connection().await.unwrap();
                sleep(Duration::new(0, MSEC_100)).await;
            })
        })
        .collect();

    for (_, hnd) in h.iter_mut().enumerate() {
        hnd.await?;
    }

    let elapsed = now.elapsed().as_secs();
    println!("elapsed {}", elapsed);
    assert!(elapsed < (COUNT as u64 / 10 + 5));

    Ok(())
}

#[tokio::test]
async fn test_ping() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;
    conn.ping().await?;

    let config = get_config().set_timeout(Duration::from_nanos(1));

    let pool = Pool::create(config).unwrap();
    let mut conn = pool.connection().await?;
    let err_timeout = conn.ping().await;

    // assert!(err_timeout.unwrap_err().is_timeout());

    let config = get_config().set_timeout(Duration::from_nanos(1));

    let pool = Pool::create(config).unwrap();
    let mut conn = pool.connection().await?;
    let _err_timeout = conn.ping().await;

    // assert!(err_timeout.unwrap_err().is_timeout());

    Ok(())
}

#[tokio::test]
async fn test_readonly_mode() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;
    conn.execute("DROP TABLE IF EXISTS test_readonly1").await?;
    conn.execute("DROP TABLE IF EXISTS test_readonly2").await?;
    let res = conn
        .execute("CREATE TABLE IF NOT EXISTS test_readonly0(id UInt64) ENGINE=Memory")
        .await;
    assert!(res.is_ok());
    drop(conn);
    drop(pool);

    let pool = get_pool_extend("tcp://localhost/?readonly=2");

    let mut conn = pool.connection().await?;
    let res = conn
        .execute("CREATE TABLE IF NOT EXISTS test_readonly1(id UInt64) ENGINE=Memory")
        .await;
    assert!(res.is_err());
    //println!("{:?}",res.unwrap_err());

    let pool = get_pool_extend("tcp://localhost/?readonly=1");
    let mut conn = pool.connection().await?;
    let res = conn
        .execute("CREATE TABLE IF NOT EXISTS test_readonly2(id UInt64) ENGINE=Memory")
        .await;

    assert!(res.is_err());

    let data1 = vec![0u64, 1, 3, 4, 5, 6];
    let block = { Block::new("test_readonly").add("id", data1) };
    let insert = conn.insert(&block).await;
    assert!(insert.is_err());
    drop(insert);

    let query_result = conn.query("SELECT count(*) FROM main").await;
    assert!(query_result.is_ok());
    let mut query_result = query_result.unwrap();
    while let Some(_block) = query_result.next().await? {}
    drop(query_result);
    drop(conn);
    drop(pool);

    Ok(())
}

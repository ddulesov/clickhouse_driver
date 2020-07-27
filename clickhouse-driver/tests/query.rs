use chrono::{DateTime, Utc};
use clickhouse_driver::prelude::errors;
use clickhouse_driver::prelude::*;
use std::env;
use std::net::Ipv4Addr;
use uuid::Uuid;

pub fn get_pool() -> Pool {
    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "tcp://localhost?execute_timeout=5s&query_timeout=20s&pool_max=4&compression=lz4".into()
    });

    Pool::create(database_url).expect("provide connection url in DATABASE_URL env variable")
}

macro_rules! get {
    ($row:ident, $idx: expr, $msg: expr) => {
        $row.value($idx)?.expect($msg)
    };
    ($row:ident, $idx: expr) => {
        get!($row, $idx, "unexpected error")
    };
}

type CHDT = DateTime<Utc>;

#[tokio::test]
async fn test_query_ddl() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;
    conn.execute("DROP TABLE IF EXISTS rust2").await?;
    conn.execute("CREATE TABLE rust2(x Int64) ENGINE=Memory")
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_query_pending() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    let mut query_result = conn.query("SELECT  i64 FROM main").await?;

    let mut i: u32 = 0;
    while let Some(_block) = query_result.next().await? {
        i += 1;
        if i == 1 {
            assert_eq!(query_result.is_pending(), true);
        }
    }

    assert_eq!(query_result.is_pending(), false);
    drop(query_result);
    Ok(())
}

#[tokio::test]
async fn test_query_string() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    let mut query_result = conn.query("SELECT title FROM main").await?;

    while let Some(block) = query_result.next().await? {
        for (j, row) in block.iter_rows().enumerate() {
            let s: &str = get!(row, 0);
            println!("{:4}:{}", j, s);
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_query_enum() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    let mut query_result = conn.query("SELECT e8,e16 FROM main").await?;

    while let Some(block) = query_result.next().await? {
        for row in block.iter_rows() {
            let e8: &str = get!(row, 0);
            let e16: &str = get!(row, 1);
            println!("'{}'='{}'", e8, e16);
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_query_nullable() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    let mut query_result = conn.query("SELECT n FROM main WHERE n=NULL").await?;

    while let Some(block) = query_result.next().await? {
        for row in block.iter_rows() {
            let n: Option<u16> = row.value(0)?;
            assert!(n.is_none());
        }
    }
    drop(query_result);

    let mut query_result = conn.query("SELECT n FROM main WHERE n=1").await?;

    while let Some(block) = query_result.next().await? {
        for row in block.iter_rows() {
            let n: Option<u16> = row.value(0)?;
            assert!(n.is_some());
            assert_eq!(n.unwrap(), 1u16);
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_query_lowcardinality() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    let mut query_result = conn.query("SELECT lcs FROM main WHERE lcs='May' LIMIT 1000").await?;

    while let Some(block) = query_result.next().await? {
        for row in block.iter_rows() {
            let lcs: &str = row.value(0)?.unwrap();
            assert_eq!(lcs, "May");
        }
    }
    drop(query_result);

    Ok(())
}

#[tokio::test]
async fn test_query_deserialize() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    #[derive(Debug)]
    struct RowObject {
        uuid: Uuid,
        title: String,
        dt: CHDT,
        ip: Ipv4Addr,
    }
    macro_rules! field {
        ($row:ident, $idx: expr, $err: expr) => {
            $row.value($idx)?.ok_or_else($err)
        };
        ($row:ident, $idx: expr) => {
            field!($row, $idx, || {
                errors::ConversionError::UnsupportedConversion
            })
        };
    }

    impl Deserialize for RowObject {
        fn deserialize(row: Row) -> errors::Result<Self> {
            let err = || errors::ConversionError::UnsupportedConversion;

            let uuid: Uuid = field!(row, 0, err)?;
            let dt: CHDT = field!(row, 1, err)?;
            let title: &str = field!(row, 2, err)?;
            let ip: Ipv4Addr = field!(row, 3, err)?;

            Ok(RowObject {
                uuid,
                dt,
                title: title.to_string(),
                ip,
            })
        }
    }

    let mut query_result = conn
        .query("SELECT  uuid, t, title, ip4 FROM main LIMIT 10 ")
        .await?;

    while let Some(block) = query_result.next().await? {
        for row in block.iter::<RowObject>() {
            println!("{:?}", row);
        }
    }

    Ok(())
}

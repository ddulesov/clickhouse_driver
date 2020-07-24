use clickhouse_driver::prelude::errors;
use clickhouse_driver::prelude::types::{Decimal32, Decimal64};
use clickhouse_driver::prelude::*;
use std::env;
use std::net::{Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

pub fn get_pool() -> Pool {
    let database_url =
        env::var("DATABASE_URL")
            .unwrap_or_else(|_| "tcp://localhost?execute_timeout=5s&query_timeout=20s&pool_max=4&compression=lz4".into());

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

#[tokio::test]
async fn test_insert_number() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("DROP TABLE IF EXISTS test_insert_number")
        .await?;
    conn.execute(
        r#"CREATE TABLE test_insert_number(
       id UInt64,
       i64 Int64,
       i32 Int32,
       u32 UInt32,
       i16 Int16,
       u16 UInt16,
       i8 Int8,
       u8 UInt8,
       f32 Float32,
       f64 Float64
       ) ENGINE=MergeTree PARTITION BY id ORDER BY id"#,
    )
    .await?;

    let data1 = vec![0u64, 1, 3, 4, 5, 6];
    let data2 = vec![0i64, 1, 3, 4, 5, 6];
    let data3 = vec![0u32, 1, 3, 4, 5, 6];
    let data4 = vec![0i32, 1, 3, 4, 5, 6];
    let data5 = vec![0u16, 1, 3, 4, 5, 6];
    let data6 = vec![0i16, 1, 3, 4, 5, 6];
    let data7 = vec![0u8, 1, 3, 4, 5, 6];
    let data8 = vec![0i8, 1, 3, 4, 5, 6];
    let data9 = vec![0.0_f32, 1., 3., 4., 5., 6.];
    let data10 = vec![0.0_f64, 1., 3., 4., 5., 6.];

    let block = {
        Block::new("test_insert_number")
            .add("id", data1)
            .add("i64", data2)
            .add("u32", data3)
            .add("i32", data4)
            .add("u16", data5)
            .add("i16", data6)
            .add("u8", data7)
            .add("i8", data8)
            .add("f32", data9)
            .add("f64", data10)
    };

    let mut insert = conn.insert(block).await?;
    insert.commit().await?;

    drop(insert);

    let mut query_result = conn
        .query(
            r#"SELECT sum(id), sum(i64),
        sum(u32), sum(i32),
        sum(u16), sum(i16),
        sum(u8), sum(i8),
        sum(f32), sum(f64)
        FROM test_insert_number"#,
        )
        .await?;
    let block = query_result.next().await?.expect("1 row block ");

    assert_eq!(block.row_count(), 1);
    let row = block.iter_rows().next().expect("first row");
    let s_id: u64 = get!(row, 0, "sum of id");
    let s_i64: i64 = get!(row, 1, "sum of i64");
    let s_u32: u64 = get!(row, 2, "sum of u32");
    let s_i32: i64 = get!(row, 3, "sum of i32");
    let s_u16: u64 = get!(row, 4, "sum of u16");
    let s_i16: i64 = get!(row, 5, "sum of i16");
    let s_u8: u64 = get!(row, 6, "sum of u8");
    let s_i8: i64 = get!(row, 7, "sum of i8");
    let s_f32: f64 = get!(row, 8, "sum of f32");
    let s_f64: f64 = get!(row, 9, "sum of f64");

    assert_eq!(s_id, 19);
    assert_eq!(s_i64, 19);
    assert_eq!(s_u32, 19);
    assert_eq!(s_i32, 19);
    assert_eq!(s_u16, 19);
    assert_eq!(s_i16, 19);
    assert_eq!(s_u8, 19);
    assert_eq!(s_i8, 19);
    assert!( (s_f32 - 19.0_f64).abs() < 0.01_f64  );
    assert!( (s_f64 - 19.0_f64).abs() < 0.01_f64 );

    Ok(())
}

#[tokio::test]
async fn test_insert_enum() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;
    let table = "test_insert_enum";

    let data_0 = vec![0u64, 1, 2, 3, 4, 5];
    let data_1 = vec!["Jun", "Jul", "Aug", "Sep", "Oct", "Nov"];
    let data_2 = vec!["Jun", "Jul", "Aug", "Sep", "Oct", "Nov"];

    let block = {
        Block::new(table)
            .add("id", data_0.clone())
            .add("e8", data_1.clone())
            .add("e16", data_2.clone())
    };

    conn.execute("DROP TABLE IF EXISTS test_insert_enum")
        .await?;
    conn.execute(
        r#"CREATE TABLE test_insert_enum(
       id UInt64,
       e8 Enum8('Jun'=6,'Jul'=7,'Aug'=8,'Sep'=9,'Oct'=10,'Nov'=11),
       e16 Enum16('Jun'=6,'Jul'=7,'Aug'=8,'Sep'=9,'Oct'=10,'Nov'=11)
       ) ENGINE=MergeTree PARTITION BY id ORDER BY id"#,
    )
    .await?;
    let mut insert = conn.insert(block).await?;
    for _ in 1u32..100u32 {
        let block = {
            Block::new(table)
                .add("id", data_0.clone())
                .add("e8", data_1.clone())
                .add("e16", data_2.clone())
        };
        insert.next(block).await?;
    }

    insert.commit().await?;

    drop(insert);
    let mut query_result = conn
        .query("SELECT sum(id), count(*) FROM test_insert_enum")
        .await?;
    let block = query_result.next().await?.expect("1 row block ");

    assert_eq!(block.row_count(), 1);
    let row = block.iter_rows().next().expect("first row");
    let s: u64 = get!(row, 0, "sum of id");
    let c: u64 = get!(row, 1, "number of rows");

    assert_eq!(s, 1500);
    assert_eq!(c, 600);
    Ok(())
}

#[tokio::test]
async fn test_insert_nullable() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;
    let table = "test_insert_nullable";

    let data_0 = vec![0u64, 1, 2, 3, 4, 5];
    let data_1 = vec![Some("Jun"), Some("Jul"), None, None, None, None];
    let data_2 = vec![Some(1u64), Some(100u64), None, None, None, None];

    let block = {
        Block::new(table)
            .add("id", data_0.clone())
            .add_nullable("s", data_1.clone())
            .add_nullable("i", data_2.clone())
    };

    conn.execute("DROP TABLE IF EXISTS test_insert_nullable")
        .await?;
    conn.execute(
        r#"CREATE TABLE test_insert_nullable(
    id UInt64,
    s Nullable(String),
    i Nullable(UInt64)
    ) ENGINE=MergeTree PARTITION BY id ORDER BY id"#,
    )
    .await?;
    let mut insert = conn.insert(block).await?;
    for _ in 1u32..100u32 {
        let block = {
            Block::new(table)
                .add("id", data_0.clone())
                .add_nullable("s", data_1.clone())
                .add_nullable("i", data_2.clone())
        };
        insert.next(block).await?;
    }

    insert.commit().await?;

    drop(insert);
    let mut query_result = conn
        .query("SELECT sum(id), count(*), sum(i) FROM test_insert_nullable")
        .await?;
    let block = query_result.next().await?.expect("1 row block ");

    assert_eq!(block.row_count(), 1);
    let row = block.iter_rows().next().expect("first row");
    let s: u64 = get!(row, 0, "sum of id");
    let c: u64 = get!(row, 1, "number of rows");
    //let s2: u64 = get!(row,2,"sum of nullable u64");

    assert_eq!(s, 1500);
    //assert_eq!(s2, 10100);
    assert_eq!(c, 600);
    Ok(())
}

#[tokio::test]
async fn test_insert_string() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;
    let table = "test_insert_string";

    let data_0 = vec![0u64, 1, 2, 3, 4, 5];
    let data_1 = vec!["Jun", "Jul", "Aug", "Sep", "Oct", "Nov"];

    let block = {
        Block::new(table)
            .add("id", data_0.clone())
            .add("s", data_1.clone())
            .add("fs", data_1.clone())
    };

    conn.execute("DROP TABLE IF EXISTS test_insert_string")
        .await?;
    conn.execute(
        r#"CREATE TABLE test_insert_string(
    id UInt64,
    s String,
    fs FixedString(3)
    ) ENGINE=MergeTree PARTITION BY id ORDER BY id"#,
    )
    .await?;
    let mut insert = conn.insert(block).await?;
    for _ in 1u32..100u32 {
        let block = {
            Block::new(table)
                .add("id", data_0.clone())
                .add("s", data_1.clone())
                .add("fs", data_1.clone())
        };
        insert.next(block).await?;
    }

    insert.commit().await?;

    drop(insert);
    let mut query_result = conn
        .query("SELECT sum(id), count(*) FROM test_insert_string")
        .await?;
    let block = query_result.next().await?.expect("1 row block ");

    assert_eq!(block.row_count(), 1);
    let row = block.iter_rows().next().expect("first row");
    let s: u64 = row.value(0)?.expect("sum of id");
    let c: u64 = row.value(1)?.expect("number of rows");

    assert_eq!(s, 1500);
    assert_eq!(c, 600);
    Ok(())
}

#[tokio::test]
async fn test_insert_date() -> errors::Result<()> {
    let now = chrono::offset::Utc::now();
    let today = chrono::offset::Utc::today();

    let pool = get_pool();
    let mut conn = pool.connection().await?;
    let table = "test_insert_date";

    let data_0 = vec![0u64, 1, 2, 3, 4, 5];
    let data_1 = vec![today; 6];
    let data_2 = vec![now; 6];
    //let data_3 = vec![now; 6];

    let block = {
        Block::new(table)
            .add("id", data_0.clone())
            .add("d", data_1.clone())
            .add("t1", data_2.clone())
            .add("t2", data_2.clone())
    };

    conn.execute("DROP TABLE IF EXISTS test_insert_date")
        .await?;
    conn.execute(
        r#"CREATE TABLE test_insert_date(
    id UInt64,
    d Date,
    t1 DateTime,
    t2 DateTime64(2)
    ) ENGINE=MergeTree PARTITION BY id ORDER BY id"#,
    )
    .await?;
    let mut insert = conn.insert(block).await?;
    for _ in 1u32..100u32 {
        let block = {
            Block::new(table)
                .add("id", data_0.clone())
                .add("d",  data_1.clone())
                .add("t1", data_2.clone())
                .add("t2", data_2.clone())
        };
        insert.next(block).await?;
    }

    insert.commit().await?;

    drop(insert);
    let mut query_result = conn
        .query("SELECT sum(id), count(*) FROM test_insert_date")
        .await?;
    let block = query_result.next().await?.expect("1 row block ");

    assert_eq!(block.row_count(), 1);
    let row = block.iter_rows().next().expect("first row");
    let s: u64 = row.value(0)?.expect("sum of id");
    let c: u64 = row.value(1)?.expect("number of rows");

    assert_eq!(s, 1500);
    assert_eq!(c, 600);
    Ok(())
}

#[tokio::test]
async fn test_insert_uuid() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;
    let table = "test_insert_uuid";

    let uuid = Uuid::new_v4();
    let data_0 = vec![0u64, 1, 2, 3, 4, 5];
    let data_1 = vec![uuid; 6];

    let block = {
        Block::new(table)
            .add("id", data_0.clone())
            .add("u", data_1.clone())
    };

    conn.execute("DROP TABLE IF EXISTS test_insert_uuid")
        .await?;
    conn.execute("CREATE TABLE test_insert_uuid(id UInt64, u UUID ) ENGINE=MergeTree PARTITION BY id ORDER BY id").await?;
    let mut insert = conn.insert(block).await?;
    for _ in 1u32..100u32 {
        let block = {
            Block::new(table)
                .add("id", data_0.clone())
                .add("u", data_1.clone())
        };
        insert.next(block).await?;
    }

    insert.commit().await?;

    drop(insert);
    let mut query_result = conn
        .query("SELECT sum(id), count(*) FROM test_insert_uuid")
        .await?;
    let block = query_result.next().await?.expect("1 row block ");

    assert_eq!(block.row_count(), 1);
    let row = block.iter_rows().next().expect("first row");
    let s: u64 = row.value(0)?.expect("sum of id");
    let c: u64 = row.value(1)?.expect("number of rows");

    assert_eq!(s, 1500);
    assert_eq!(c, 600);
    Ok(())
}

#[tokio::test]
async fn test_insert_decimal() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;
    let table = "test_insert_decimal";

    let data_0 = vec![0u64, 1, 2, 3, 4, 5];
    let data_1 = vec![Decimal32::from(10001, 3); 6];
    let data_2 = vec![Decimal64::from(10001, 3); 6];

    let block = {
        Block::new(table)
            .add("id", data_0.clone())
            .add("d32", data_1.clone())
            .add("d64", data_2.clone())
    };

    conn.execute("DROP TABLE IF EXISTS test_insert_decimal")
        .await?;
    conn.execute(
        r#"CREATE TABLE test_insert_decimal(
     id UInt64,
     d32 Decimal32(3),
     d64 Decimal64(3)
    ) ENGINE=MergeTree PARTITION BY id ORDER BY id"#,
    )
    .await?;
    let mut insert = conn.insert(block).await?;
    for _ in 1u32..100u32 {
        let block = {
            Block::new(table)
                .add("id", data_0.clone())
                .add("d32", data_1.clone())
                .add("d64", data_2.clone())
        };
        insert.next(block).await?;
    }

    insert.commit().await?;

    drop(insert);
    let mut query_result = conn
        .query("SELECT sum(id), count(*) FROM test_insert_decimal")
        .await?;
    let block = query_result.next().await?.expect("1 row block ");

    assert_eq!(block.row_count(), 1);
    let row = block.iter_rows().next().expect("first row");
    let s: u64 = row.value(0)?.expect("sum of id");
    let c: u64 = row.value(1)?.expect("number of rows");

    assert_eq!(s, 1500);
    assert_eq!(c, 600);
    Ok(())
}

#[tokio::test]
async fn test_insert_ip() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;
    let table = "test_insert_ip";

    let ip4: Ipv4Addr = "127.0.0.1".parse().unwrap();
    let ip6 = Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0x1);

    assert_eq!(ip6.is_loopback(), true);
    assert_eq!(ip4.is_loopback(), true);
    let data_0 = vec![0u64, 1, 2, 3, 4, 5];
    let data_1 = vec![ip4; 6];
    let data_2 = vec![ip6; 6];

    let block = {
        Block::new(table)
            .add("id", data_0.clone())
            .add("ip4", data_1.clone())
            .add("ip6", data_2.clone())
    };

    conn.execute("DROP TABLE IF EXISTS test_insert_ip").await?;
    conn.execute(
        r#"CREATE TABLE test_insert_ip(
    id UInt64,
    ip4 IPv4,
    ip6 IPv6
    ) ENGINE=MergeTree PARTITION BY id ORDER BY id"#,
    )
    .await?;
    let mut insert = conn.insert(block).await?;

    for _ in 1u32..100u32 {
        let block = {
            Block::new(table)
                .add("id", data_0.clone())
                .add("ip4", data_1.clone())
                .add("ip6", data_2.clone())
        };
        insert.next(block).await?;
    }

    insert.commit().await?;

    drop(insert);
    let mut query_result = conn
        .query("SELECT sum(id), count(*) FROM test_insert_ip")
        .await?;
    let block = query_result.next().await?.expect("1 row block ");

    assert_eq!(block.row_count(), 1);
    let row = block.iter_rows().next().expect("first row");
    let s: u64 = row.value(0)?.expect("sum of id");
    let c: u64 = row.value(1)?.expect("number of rows");

    assert_eq!(s, 1500);
    assert_eq!(c, 600);

    Ok(())
}

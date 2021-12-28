extern crate chrono;
extern crate clickhouse_driver;
extern crate tokio;
extern crate uuid;

use clickhouse_driver::prelude::*;
use std::net::Ipv4Addr;
use std::{env, io, time};

macro_rules! get {
    ($row: ident, $i: expr, $err: ident) => {
        $row.value($i)?.ok_or_else($err)?;
    };
    ($row: ident, $i: expr, $err: ident, opt) => {
        $row.value($i)?;
    };
}
// CREATE table mainx(
//   i64 UInt64,
//   lcs LowCardinality(String),
//   aip4 Array(IPv4)
// ) ENGINE=Memory
#[derive(Debug)]
struct ClickhouseRow {
    id: i64,
    name: Option<String>,
    ip: Vec<Ipv4Addr>,
}

impl Deserialize for ClickhouseRow {
    fn deserialize(row: Row) -> errors::Result<Self> {
        let err = || errors::ConversionError::UnsupportedConversion;

        let id: i64 = get!(row, 0, err);
        let name: Option<String> = get!(row, 1, err, opt).map(|s: &str| s.to_owned());
        let ip: Vec<Ipv4Addr> = get!(row, 2, err);

        Ok(ClickhouseRow { id, name, ip })
    }
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());

    let pool = Pool::create(database_url.as_str())?;
    {
        let mut start = time::Instant::now();
        let mut conn = pool.connection().await?;
        eprintln!(
            "connection establish in {} msec",
            start.elapsed().as_millis()
        );
        start = time::Instant::now();

        let mut result = conn.query("SELECT i64, lcs, aip4 FROM mainx").await?;

        let mut c = 0;
        while let Some(block) = result.next().await? {
            for item in block.iter::<ClickhouseRow>() {
                println!("{:?}", item);
                c += 1;
            }
        }
        eprintln!("fetch {} rows  in {} msec", c, start.elapsed().as_millis());
        eprintln!("progress {:?}", result.progress);
    }

    Ok(())
}

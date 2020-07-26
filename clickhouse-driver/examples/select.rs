extern crate chrono;
extern crate clickhouse_driver;
extern crate tokio;
extern crate uuid;

use std::{env, io, time};
use clickhouse_driver::prelude::*;

type ServerDate = chrono::DateTime<chrono::Utc>;

#[derive(Debug)]
struct Perf {
    id: u32,
    name: String,
    date: ServerDate,
}

macro_rules! get{
    ($row: ident, $i: expr, $err: ident)=>{
        $row.value($i)?.ok_or_else( $err )?;
    }
}

impl Deserialize for Perf {
    fn deserialize(row: Row) -> errors::Result<Self> {
        let err = || errors::ConversionError::UnsupportedConversion;

        let id: u32 = get!(row,0, err);
        let name: &str = get!(row,1, err);
        let date: ServerDate = get!(row,2, err);

        Ok(Perf {
            id,
            name: name.to_string(),
            date,
        })
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
        eprintln!("connection establish {} msec", start.elapsed().as_millis());
        start = time::Instant::now();

        let mut result = conn
            .query("SELECT id, name, dt FROM perf")
            .await?;

        let mut sum: u64 = 0;
        while let Some(block) = result.next().await? {
            /*
            for  item in block.iter::<Perf>() {
                sum += item.id as u64;
            }
            */
        }
        eprintln!("fetch block {} msec", start.elapsed().as_millis());
        eprintln!("{}", sum);
        drop(sum);
    }

    Ok(())
}

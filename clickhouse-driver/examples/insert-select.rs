extern crate chrono;
extern crate clickhouse_driver;
extern crate tokio;
extern crate uuid;

use std::net::Ipv4Addr;
use std::{env, io};

use uuid::Uuid;

use clickhouse_driver::prelude::types::Decimal;
use clickhouse_driver::prelude::*;

type ServerDate = chrono::DateTime<chrono::Utc>;

#[derive(Debug)]
struct Blob {
    id: u64,
    url: String,
    date: ServerDate,
    client: Uuid,
    ip: Ipv4Addr,
    //value: Decimal,
}

impl Deserialize for Blob {
    fn deserialize(row: Row) -> errors::Result<Self> {
        let err = || errors::ConversionError::UnsupportedConversion;

        let id: u64 = row.value(0)?.ok_or_else(err)?;
        let url: &str = row.value(1)?.ok_or_else(err)?;
        let date: ServerDate = row.value(2)?.ok_or_else(err)?;
        let client: Uuid = row.value(3)?.ok_or_else(err)?;
        let ip = row.value(4)?.ok_or_else(err)?;
        //let value: Decimal = row.value(5)?.ok_or_else(err)?;

        Ok(Blob {
            id,
            date,
            client,
            //value,
            url: url.to_string(),
            ip,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let ddl = "
        CREATE TABLE IF NOT EXISTS blob (
            id          UInt64,
            url         String,
            date        DateTime,
            client      UUID,
            ip          IPv4,
            value       Decimal32(2)
        ) ENGINE=MergeTree PARTITION BY id ORDER BY date";
    //

    let uuid = Uuid::new_v4();
    let ip: Ipv4Addr = "127.0.0.1".parse().unwrap();
    let value = Decimal::from(4000_i32, 2);
    let now = chrono::offset::Utc::now();
    //let today = chrono::offset::Utc::today();

    let id = vec![0u64, 159, 146, 150];
    let url = vec![
        "https://www.rust-lang.org/",
        "https://tokio.rs/",
        "https://github.com/ddulesov/",
        "https://internals.rust-lang.org/",
    ];
    let date = vec![now; 4];
    let client = vec![uuid; 4];
    let ip = vec![ip; 4];
    let value = vec![value; 4];

    let block = {
        Block::new("blob")
            .add("id", id.clone())
            .add("url", url.clone())
            .add("date", date.clone())
            .add("client", client.clone())
            .add("ip", ip.clone())
            .add("value", value.clone())
    };

    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());

    let pool = Pool::create(database_url.as_str())?;
    {
        let mut conn = pool.connection().await?;

        conn.execute("DROP TABLE IF EXISTS blob").await?;
        conn.execute(ddl).await?;
        let mut insert = conn.insert(block).await?;
        println!("INSERT...");
        for _ in 1u64..1000000 {
            let block = {
                Block::new("")
                    .add("id", id.clone())
                    .add("url", url.clone())
                    .add("date", date.clone())
                    .add("client", client.clone())
                    .add("ip", ip.clone())
                    .add("value", value.clone())
            };
            insert.next(block).await?;
        }

        insert.commit().await?;
        // Stop inserting pipeline before  next query be called
        drop(insert);

        println!("SELECT...");
        let mut result = conn
            .query("SELECT id, url, date, client, ip FROM blob WHERE id=150  ORDER BY date LIMIT 30000")
            .await?;

        while let Some(block) = result.next().await? {
            for (i, row) in block.iter::<Blob>().enumerate() {
                if i % 100 == 0 {
                    println!("{:5} {:?}", i, row);
                }
            }
        }
    }

    Ok(())
}

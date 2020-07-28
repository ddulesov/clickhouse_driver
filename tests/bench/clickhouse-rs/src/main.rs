extern crate tokio;
extern crate chrono;
extern crate chrono_tz;

extern crate clickhouse_rs;
use clickhouse_rs::{Block, Pool, ClientHandle};
use std::io;
use std::env;
use std::time;
use chrono::prelude::*;
use chrono_tz::Tz;
use chrono_tz::Tz::UTC;

static NAMES: [&str; 5] = ["one", "two", "three", "four", "five"];
const BSIZE: u64 = 10000;
const CIRCLE: u64 = 1000;

/// Add some entropy to mess up LZ4 compression

fn next_block(i: u64) -> Block {
	let now = UTC.ymd(2016, 10, 22).and_hms(12, 0, 0);
	//let now = chrono::offset::Utc::now();
    //let now = SystemTime::now();
    //let now: u64 = now.duration_since(time::UNIX_EPOCH).unwrap().as_secs();

    let dt: Vec<_> = (0..BSIZE).map(|idx| now + chrono::Duration::seconds(idx as i64) ).collect();

    let name: Vec<&str> = (0..BSIZE)
        .map(|idx| NAMES[idx as usize % NAMES.len()])
        .collect();

    Block::new()
        .column("id", vec![i as u32; BSIZE as usize])
        .column("name", name)
        .column("dt", dt)
}

async fn insert_perf(mut client: ClientHandle) -> io::Result<()>{
	let ddl = r"
	CREATE TABLE IF NOT EXISTS perf_rust1 (
		id  UInt32,
		name  String,
		dt   DateTime
	) Engine=MergeTree PARTITION BY name ORDER BY dt";

	client.execute("DROP TABLE IF EXISTS perf_rust1").await?;
	
    client.execute(ddl).await?;
    let start = time::Instant::now();
    for i in 0u64..CIRCLE {
        let block = next_block(i);
        client.insert("perf_rust1", block).await?;
    }

    eprintln!("elapsed {} msec", start.elapsed().as_millis());
    eprintln!("{} rows have been inserted", BSIZE * CIRCLE);
	Ok(())
}

async fn select_perf(mut client: ClientHandle) -> io::Result<()> {
	let start = time::Instant::now();
	let block = client.query("SELECT id,name,dt FROM perf").fetch_all().await?;
	let mut sum: u64 = 0;
	
	for row in block.rows() {
        let id: u32             = row.get("id")?;
        let _name: String        = row.get("name")?;
        let _dt: DateTime<Tz> 				= row.get("dt")?;
        
		sum += id as u64;
    }
	eprintln!("elapsed {} msec", start.elapsed().as_millis());
    eprintln!("{} ", sum);
	
	
	Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
	let mut args = env::args();
	args.next();
	let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());
		
	println!("connect to {}", &database_url);
    let pool = Pool::new(database_url);
	let client = pool.get_handle().await?;
	
	if let Some(meth) = args.next() {
		match meth.as_str() {
			"insert" => insert_perf(client).await,
			"select" => select_perf(client).await,
			_ => panic!("provide perf method 'insert' or 'select'")
		}
		
	}else{
		panic!("provide perf method 'insert' or 'select'")
	}
	   
}

extern crate chrono;
extern crate clickhouse_driver;
extern crate tokio;

use std::{env, io, time};
use clickhouse_driver::prelude::*;

static NAMES: [&str;5] = ["one","two","three","four","five"];
const BSIZE: u64 = 10000;
const CIRCLE: u64 = 1000;

fn next_block(i: u64)->Block<'static> {
	let now = chrono::offset::Utc::now();

	let dt: Vec<_> = (0..BSIZE).map(|idx| now + chrono::Duration::seconds(idx as i64) ).collect();
	let name: Vec<_> = (0..BSIZE).map(|idx| NAMES[ idx as usize % NAMES.len() ] ).collect();
			
	Block::new("perf_rust2")
		.add("id",vec![i as u32; BSIZE as usize] )
		.add("name", name)
		.add("dt", dt )
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let ddl = r"
        CREATE TABLE IF NOT EXISTS perf_rust2 (
            id  UInt32,
            name  String,
            dt   DateTime
        ) Engine=MergeTree PARTITION BY name ORDER BY dt";


    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());

	let pool = Pool::create(database_url.as_str())?;

	let mut conn = pool.connection().await?;

	conn.execute("DROP TABLE IF EXISTS perf_rust2").await?;
	conn.execute( ddl ).await?;
	
	let start = time::Instant::now();
	
	let mut insert = conn.insert( next_block(0) ).await?;

	for i in 1u64..CIRCLE{
		
		insert.next(next_block(i)).await?;
	}

	insert.commit().await?;
	// Stop inserting pipeline before  next query be called
	drop(insert);

	eprintln!("elapsed {} msec", start.elapsed().as_millis());
	eprintln!("{} rows have been inserted", BSIZE * CIRCLE);        
    

    Ok(())
}

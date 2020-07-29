# ClickHouse Rust Client #
[![Build Status](https://travis-ci.org/ddulesov/clickhouse_driver.svg?branch=master)](https://travis-ci.org/ddulesov/clickhouse_driver)
![Rust](https://github.com/ddulesov/clickhouse_driver/workflows/Rust/badge.svg?branch=master)

Asynchronous pure rust tokio-based  Clickhouse client library 
 
development status: **alpha** 

Tested on Linux x86-64 (ubuntu 20.04 LTS), Windows 10.

### Why ###

* To create small, quick, and robust driver for Clickhouse, fast open-source column oriented database
* To learn rust concurrency and zero-cost abstraction

### Supported features ###

* Asynchronous tokio-based engine
* Native Clickhouse protocol
* LZ4 compression
* Persistent connection pool
* Simple row to object mapper
* Date | DateTime | DateTime64- read/write
* (U)Int(8|16|32|64) - read/write
* Float32 | Float64 - read/write
* UUID - read/write
* String | FixedString- read/write
* Ipv4 | Ipv6 - read/write
* Nullable(*) - read/write
* Decimal - read/write
* Enum8, Enum16 - read/write

### Use cases ###

* Make query using SQL syntax supported by Clickhouse Server 
* Execute arbitrary DDL commands  
* Query Server status
* Insert into Clickhouse Server big (possibly continues ) data stream
* Load-balancing using round-robin method

### Quick start ###
The package has not published in crates.io.
Download source from [home git](https://github.com/ddulesov/clickhouse_driver)

Building requires rust 1.41 stable or nightly,
tokio-0.2.x.

- Add dependencies to Cargo.toml 
  ```toml   
  clickhouse-driver = { version="0.1.0-alpha.1", path="../path_to_package"}
  clickhouse-driver-lz4 = { version="0.1.0", path="../path_to_package"}
  clickhouse-driver-cthrs = { version="0.1.0", path="../path_to_package"}
  ```
- Add usage in main.rs
  ```rust
  extern crate clickhouse_driver;   
  use clickhouse_driver::prelude::*;
  ```
  
to connect to server provide connection url 
```
tcp://username:password@localhost/database?paramname=paramvalue&...
```
for example
```
tcp://user:default@localhost/log?ping_timout=200ms&execute_timeout=5s&query_timeout=20s&pool_max=4&compression=lz4
```
### parameters
* `compression` - accepts 'lz4' or 'none'.
   lz4 - fast and efficient compression method.
   It can significantly reduce transmitted data size and time if used for
   big data chunks. For small data it's better to choose none compression;
   
* `connection_timeout` - timeout for establishing connection.
   Default is 500ms;
       
* `execute_timeout` - timeout for waiting result of **execute** method call
   If the execute  used for alter huge table it can take 
   long time to complete. In this case  set this parameter to appropriate
   value. In other cases leave the  default value (180 sec);
      
* `query_timout` - timeout for waiting response from the server with
   next block of data in **query** call.
   Note. Large data query may take long time. This timeout requires that only 
   one chunk of data will receive until the end of timeout.
   Default value is 180sec;
   
* `insert_timeout` - timeout for waiting result of `insert` call
   insert method call returns error if the server does not receive
   message until the end of insert_timeout.
   As insert data processing is asynchronous it doesn't include server block processing time.
   Default value is 180 sec;
   
* `ping_timout` - wait before ping response.
   The host will be considered unavailable if the server  
   does not return pong response until the end of ping_timeout; 
      
* `retry_timeout` - the number of seconds to wait before send next ping 
   if the server does not return;  
   
* `ping_before_query` - 1 (default) or 0.  This option if set 
   requires the driver to check Clickhouse server  responsibility 
   after returning connection from pool;
   
* `pool_min` - minimal connection pool size. 
   the number of idle connections that can be kept in the pool;
   Default value is 2;
   
* `pool_max` - maximum number of established connections that the pool 
   can issued.  If the task require new connection while the pool reaches the maximum
   and there is not idle connection then this task will be put in waiting queue.
   Default value is 10;
   
* `readonly` - 0 (default) |1|2. 
   0 - all commands allowed. 
   2- select queries and change settings, 
   1 - only select queries ;
   
* `keepalive` - keepalive TCP option;

* `host` - alternative host(s)

All timeout parameters accept integer number - the number of seconds.
To specify timeout in milliseconds add `ms` at the end.
Examples: 
 - `200ms`  ( 200 mseconds )
 - `20`     ( 20 seconds )
 - `10s`    ( 10 seconds )
 
### Example
```rust  

struct Blob {
    id: u64,
    url: String,
    date: ServerDate,
    client: Uuid,
    ip: Ipv4Addr,
    value: Decimal32,
}

impl Deserialize for Blob {
    fn deserialize(row: Row) -> errors::Result<Self> {
        let err = || errors::ConversionError::UnsupportedConversion;

        let id: u64 = row.value(0)?.ok_or_else(err)?;
        let url: &str = row.value(1)?.ok_or_else(err)?;
        let date: ServerDate = row.value(2)?.ok_or_else(err)?;
        let client: Uuid = row.value(3)?.ok_or_else(err)?;
        let ip = row.value(4)?.ok_or_else(err)?;
        let value: Decimal32 = row.value(5)?.ok_or_else(err)?;

        Ok(Blob {
            id,
            date,
            client,
            value,
            url: url.to_string(),
            ip,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {

    //    CREATE TABLE IF NOT EXISTS blob (
    //        id          UInt64,
    //        url         String,
    //        date        DateTime,
    //        client      UUID,
    //        ip          IPv4
    //    ) ENGINE=MergeTree PARTITION BY id ORDER BY date

    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());

    let pool = Pool::create(database_url.as_str())?;
    {
        let mut conn = pool.connection().await?;
        conn.ping().await?;

        let mut result = conn
            .query("SELECT id, url, date, client, ip FROM blob WHERE id=150  ORDER BY date LIMIT 30000")
            .await?;

        while let Some(block) = result.next().await? {
            for blob in block.iter::<Blob>() {
                ...
            }
        }
    }

    Ok(())
}
```

* For more examples see clickhouse-driver/examples/ directory*

### Known issues and limitations ###

* Doesn't support Array data type, 
* LowCardinality - readonly and just String base type
* Insert method support only limited data types 
  `insert` requires that inserted data  exactly matches table column type
   - Int8(16|32|64)  - i8(16|32|64)
   - UInt8(16|32|64) - u8(16|32|64)
   - Float32 - f32
   - Float64 - f64
   - Date    - chrono::Date<Utc>
   - DateTime - chrono::DateTime<Utc>
   - UUID - Uuid
   - IPv4 - AddrIpv4
   - IPv6 - AddrIpv6
   - String - &str or String or &[u8]
   - Enum8|16 - &str or String
   
### Roadmap

* Array column data type - read/write  
* Tuple - no plans to  support 
* AggregateFunction - no plans to support
* LowCardinality - add write support, extend feature to Date, DateTime types   
* Serde - serializer/deserializer interface    
   
   
  

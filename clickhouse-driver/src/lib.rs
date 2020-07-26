#![recursion_limit = "128"]
extern crate byteorder;
extern crate chrono;
extern crate chrono_tz;
#[cfg(not(feature = "cityhash_rs"))]
extern crate clickhouse_cityhash;
#[cfg(feature = "cityhash_rs")]
extern crate clickhouse_cityhash_rs;
extern crate clickhouse_derive;
extern crate core;
#[macro_use]
extern crate futures;
extern crate hostname;
#[macro_use]
extern crate lazy_static;
extern crate log;
#[cfg(lz4)]
extern crate lz4a;
#[cfg(test)]
extern crate rand;
extern crate thiserror;
extern crate tokio;
extern crate url;
extern crate uuid;

use pool::options::Options;
//use crate::protocol::Encoder;

#[cfg(not(target_endian = "little"))]
compile_error!("only little-endian platforms supported");

mod client;
mod compression;
mod errors;
mod pool;
pub mod prelude;
mod protocol;
mod types;

#[allow(dead_code)]
const MAX_STRING_LEN: usize = 64 * 1024;
/// Max number of rows in server block, 640K is default value
const MAX_BLOCK_SIZE: usize = 640 * 1024;
/// Max size of server block, bytes, 1M is default value
const MAX_BLOCK_SIZE_BYTES: usize = 10 * 1024 * 1024;
/// Connection state flags
const FLAG_DETERIORATED: u8 = 0x01;
const FLAG_PENDING: u8 = 0x02;

pub static CLIENT_NAME: &str = "Rust Native Driver";
pub const CLICK_HOUSE_REVISION: u64 = 54405;
pub const CLICK_HOUSE_DBMSVERSION_MAJOR: u64 = 1;
pub const CLICK_HOUSE_DBMSVERSION_MINOR: u64 = 1;

lazy_static! {
    static ref HOSTNAME: String = {
        hostname::get().map_or_else(
            |_orig| String::new(),
            |s| s.into_string().unwrap_or_default(),
        )
    };
    static ref DEF_OPTIONS: Options = crate::pool::options::Options::default();
}

pub fn description() -> String {
    format!(
        "{} {}.{}.{}",
        CLIENT_NAME,
        CLICK_HOUSE_DBMSVERSION_MAJOR,
        CLICK_HOUSE_DBMSVERSION_MINOR,
        CLICK_HOUSE_REVISION
    )
}

#[test]
fn test_encoder() {
    assert_eq!(description(), "Rust Native Driver 1.1.54405");
}

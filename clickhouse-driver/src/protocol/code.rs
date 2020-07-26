//pub const DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO: u64 = 54060;
//pub const DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS: u64 = 54429;

pub const DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES: u32 = 50264;
pub const DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS: u32 = 51554;
pub const DBMS_MIN_REVISION_WITH_BLOCK_INFO: u32 = 51903;
pub const DBMS_MIN_REVISION_WITH_CLIENT_INFO: u32 = 54032;
pub const DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE: u32 = 54058;
pub const DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO: u32 = 54060;
//pub const DBMS_MIN_REVISION_WITH_TABLES_STATUS            54226
//pub const DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE 54337
pub const DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME: u32 = 54372;
pub const DBMS_MIN_REVISION_WITH_VERSION_PATCH: u32 = 54401;
#[allow(dead_code)]
pub const DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE: u32 = 54405;

// Client message packet types
pub const CLIENT_HELLO: u64 = 0;
pub const CLIENT_QUERY: u64 = 1;
pub const CLIENT_DATA: u64 = 2;
pub const CLIENT_CANCEL: u64 = 3;
pub const CLIENT_PING: u64 = 4;

pub const STATE_COMPLETE: u8 = 2;

// Server message packet types
pub const SERVER_HELLO: u64 = 0;
pub const SERVER_DATA: u64 = 1;
pub const SERVER_EXCEPTION: u64 = 2;
pub const SERVER_PROGRESS: u64 = 3;
pub const SERVER_PONG: u64 = 4;
pub const SERVER_END_OF_STREAM: u64 = 5;
pub const SERVER_PROFILE_INFO: u64 = 6;
// pub const SERVER_TOTALS: u64 = 7;
// pub const SERVER_EXTREMES: u64 = 8;

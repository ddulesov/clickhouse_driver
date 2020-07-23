use super::code::*;
use crate::protocol::block::ServerBlock;
use chrono_tz::Tz;

pub(crate) struct ProfileInfo {
    pub rows: u64,
    pub bytes: u64,
    pub blocks: u64,
    pub applied_limit: u8,
    pub rows_before_limit: u64,
    pub calculated_rows_before_limit: u8,
}

impl std::fmt::Debug for ProfileInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProfileInfo")
            .field("rows", &self.rows)
            .field("bytes", &self.bytes)
            .field("blocks", &self.blocks)
            .field("application limit", &self.applied_limit)
            .field("rows before limit", &self.rows_before_limit)
            .field(
                "calculated rows before limit",
                &self.calculated_rows_before_limit,
            )
            .finish()
    }
}

pub struct Statistics {
    rows: u64,
    bytes: u64,
    total: u64,
}

impl std::fmt::Debug for Statistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Statistics")
            .field("rows", &self.rows)
            .field("bytes", &self.bytes)
            .field("total", &self.total)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) enum Response {
    Pong,
    Data(ServerBlock),
    Hello(String, u64, u64, u64, Tz),
    // Eos,
    // Profile(u64, u64, u64, u8, u8),
    // Totals,
    // Extremes,
}

impl Response {
    pub(crate) fn code(&self) -> u64 {
        match self {
            Response::Pong => SERVER_PONG,
            Response::Data(_) => SERVER_DATA,
            Response::Hello(..) => SERVER_HELLO,
            // Response::Eos => SERVER_END_OF_STREAM,
            // Response::Extremes => SERVER_EXTREMES,
            // Response::Totals => SERVER_TOTALS,
            // Response::Excepion(..) => SERVER_EXCEPTION,
        }
    }
}

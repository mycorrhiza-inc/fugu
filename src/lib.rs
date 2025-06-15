#![allow(unused_imports, unused_variables, unused_mut, dead_code, unused_must_use)]

#![allow(dead_code, unused_imports, unused_variables, unused_mut, unused_must_use)]

pub mod cli;
pub mod db;
pub mod object;
pub mod query_endpoints;
pub mod server;
// pub mod time_index;
#[cfg(test)]
mod document_statistics_test;
#[cfg(test)]
mod query_test;
mod s3;
#[cfg(test)]
mod test_inverted_index;
mod tokeinze;
pub mod tracing_utils;
pub use object::ObjectRecord;

#[cfg(test)]
mod db_test;

use serde_json::json;
use tracing::{debug, info};

/// Write-Ahead Log (WAL) implementation for ensuring data durability
///
/// The WAL provides:
/// - Persistence of operations before they're applied to indexes
/// - Recovery capability in case of crashes
/// - Atomic operations for consistent state
use std::fs;
use std::path::PathBuf;
// use tokio::time::{sleep, Duration};
use rkyv::{rancor::Error, Archive, Deserialize, Serialize};
/// WAL operations that can be recorded in the log
///
/// These operations represent the possible mutations to the system:
/// - Put: Insert or update a key-value pair
/// - Delete: Remove a key-value pair
/// - Patch: Partially update a key-value pair
#[derive(Archive, Debug, Deserialize, Serialize, Clone)]
pub enum WALOP {
    /// Insert or update a key-value pair
    Put { key: String, value: Vec<u8> },
    /// Remove a key by its identifier
    Delete { key: String },
    /// Partially update an existing key-value pair
    Patch { key: String, value: Vec<u8> },
}
/// Conversion from WAL commands to WAL operations
impl Into<WALOP> for WALCMD {
    fn into(self) -> WALOP {
        match self {
            WALCMD::Put { key, value } => WALOP::Put { key, value },
            WALCMD::Delete { key } => WALOP::Delete { key },
            WALCMD::Patch { key, value } => WALOP::Patch { key, value },
            WALCMD::DumpWAL { response: _ } => panic!("Cannot convert DumpWAL command to WALOP"),
        }
    }
}

/// Conversion from WAL operations to WAL commands
impl From<WALOP> for WALCMD {
    fn from(op: WALOP) -> Self {
        match op {
            WALOP::Put { key, value } => WALCMD::Put { key, value },
            WALOP::Delete { key } => WALCMD::Delete { key },
            WALOP::Patch { key, value } => WALCMD::Patch { key, value },
        }
    }
}

/// Commands that can be sent through the WAL channel
///
/// These commands represent both:
/// - Operations that modify data (Put, Delete, Patch)
/// - Management operations (DumpWAL)
#[derive(Debug)]
pub enum WALCMD {
    /// Insert or update a key-value pair
    Put {
        /// The key to insert or update
        key: String,
        /// The value to store
        value: Vec<u8>,
    },
    /// Delete a key-value pair
    Delete {
        /// The key to delete
        key: String,
    },
    /// Partially update an existing key-value pair
    Patch {
        /// The key to update
        key: String,
        /// The updated value
        value: Vec<u8>,
    },
    /// Dump the WAL contents for debugging or inspection
    DumpWAL {
        /// Channel to send the WAL dump response
        response: tokio::sync::oneshot::Sender<String>,
    },
}

/// Write-Ahead Log (WAL) structure for durability
///
/// The WAL records all operations in order and persists them to disk
/// before they're applied to the indexes, ensuring that data can be
/// recovered after a crash or unexpected shutdown.
#[derive(Archive, Deserialize, Serialize, Clone)]
pub struct WAL {
    /// Path where the WAL file is stored
    path: String,
    /// List of operations in the log
    log: Vec<WALOP>,
}

impl WAL {
    /// Opens or creates a new WAL at the specified path
    ///
    /// # Arguments
    ///
    /// * `path` - Path where the WAL file will be stored
    ///
    /// # Returns
    ///
    /// A new WAL instance
    pub fn open(path: PathBuf) -> WAL {
        let s = path.into_os_string().into_string().unwrap();
        return WAL {
            path: s,
            log: vec![],
        };
    }
    pub fn save(&self) -> Result<(), Error> {
        let buf = rkyv::to_bytes::<Error>(&self.log).unwrap();
        fs::write(PathBuf::from(self.path.clone()), buf).expect("Unable to write file");
        Ok(())
    }
    pub fn push(&mut self, msg: WALOP) -> Result<(), Error> {
        self.log.push(msg);
        self.save()
    }

    pub fn dump(&self) -> Result<String, Error> {
        let mut output = String::new();
        for (i, op) in self.log.iter().enumerate() {
            output.push_str(&format!("{}. {:?}\n", i + 1, op));
        }
        Ok(output)
    }
}

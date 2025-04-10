use std::fs;
use std::path::PathBuf;
// use tokio::time::{sleep, Duration};
use rkyv::{rancor::Error, Archive, Deserialize, Serialize};
#[derive(Archive, Debug, Deserialize, Serialize, Clone)]
pub enum WALOP {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
    Patch { key: String, value: Vec<u8> },
}
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

impl From<WALOP> for WALCMD {
    fn from(op: WALOP) -> Self {
        match op {
            WALOP::Put { key, value } => WALCMD::Put { key, value },
            WALOP::Delete { key } => WALCMD::Delete { key },
            WALOP::Patch { key, value } => WALCMD::Patch { key, value },
        }
    }
}

#[derive(Debug)]
pub enum WALCMD {
    Put {
        key: String,
        value: Vec<u8>,
    },
    Delete {
        key: String,
    },
    Patch {
        key: String,
        value: Vec<u8>,
    },
    DumpWAL {
        response: tokio::sync::oneshot::Sender<String>,
    },
}

#[derive(Archive, Deserialize, Serialize, Clone)]
pub struct WAL {
    path: String,
    log: Vec<WALOP>,
}

impl WAL {
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

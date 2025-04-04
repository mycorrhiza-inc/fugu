use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::SendError;
use std::{collections::HashMap, fmt};
use tokio::sync::mpsc;
// use tokio::time::{sleep, Duration};
use rkyv::{rancor::Error, Archive, Deserialize, Serialize};

use clap::{arg, command, value_parser, ArgAction, ArgMatches, Command, Subcommand};
use sled;

#[derive(Archive, Debug, Deserialize, Serialize)]
// We have a recursive type, which requires some special handling
//
// First the compiler will return an error:
//
// > error[E0275]: overflow evaluating the requirement `HashMap<String,
// > JsonValue>: Archive`
//
// This is because the implementation of Archive for Json value requires that
// JsonValue: Archive, which is recursive!
// We can fix this by adding #[omit_bounds] on the recursive fields. This will
// prevent the derive from automatically adding a `HashMap<String, JsonValue>:
// Archive` bound on the generated impl.
//
// Next, the compiler will return these errors:
// > error[E0277]: the trait bound `__S: ScratchSpace` is not satisfied
// > error[E0277]: the trait bound `__S: Serializer` is not satisfied
//
// This is because those bounds are required by HashMap and Vec, but we removed
// the default generated bounds to prevent a recursive impl.
// We can fix this by manually specifying the bounds required by HashMap and Vec
// in an attribute, and then everything will compile:
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
// We need to manually add the appropriate non-recursive bounds to our
// `CheckBytes` derive. In our case, we need to bound
// `__C: rkyv::validation::ArchiveContext`. This will make sure that our `Vec`
// and `HashMap` have the `ArchiveContext` trait implemented on the validator.
// This is a necessary requirement for containers to check their bytes.
//
// With those two changes, our recursive type can be validated with `access`!
#[rkyv(bytecheck(
    bounds(
        __C: rkyv::validation::ArchiveContext,
    )
))]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
    Array(#[rkyv(omit_bounds)] Vec<JsonValue>),
    Object(#[rkyv(omit_bounds)] HashMap<String, JsonValue>),
}

impl fmt::Display for ArchivedJsonValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "null")?,
            Self::Bool(b) => write!(f, "{}", b)?,
            Self::Number(n) => write!(f, "{}", n)?,
            Self::String(s) => write!(f, "{}", s)?,
            Self::Array(a) => {
                write!(f, "[")?;
                for (i, value) in a.iter().enumerate() {
                    write!(f, "{}", value)?;
                    if i < a.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "]")?;
            }
            Self::Object(h) => {
                write!(f, "{{")?;
                for (i, (key, value)) in h.iter().enumerate() {
                    write!(f, "\"{}\": {}", key, value)?;
                    if i < h.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "}}")?;
            }
        }
        Ok(())
    }
}

#[derive(Archive, Debug, Deserialize, Serialize)]
pub enum JsonNumber {
    PosInt(u64),
    NegInt(i64),
    Float(f64),
}

impl fmt::Display for ArchivedJsonNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PosInt(n) => write!(f, "{}", n),
            Self::NegInt(n) => write!(f, "{}", n),
            Self::Float(n) => write!(f, "{}", n),
        }
    }
}

struct FuguConfig {
    dir: PathBuf,
}

struct Fugu {
    config: FuguConfig,
    wal: WAL,
}
impl Fugu {
    fn up() {}
    fn down() {}
}

struct NodeConfig {}

#[derive(Archive, Debug, Deserialize, Serialize)]
enum WALOP {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
    Patch { key: String, value: Vec<u8> },
}

#[derive(Archive, Deserialize, Serialize)]
struct WAL {
    path: String,
    log: Vec<WALOP>,
}

impl WAL {
    fn open(path: PathBuf) -> WAL {
        // - [ ] check path
        // - [ ] attempt to deserialize
        // - [ ] if fail, quit
        let s = path.into_os_string().into_string().unwrap();
        return WAL {
            path: s,
            log: vec![],
        };
    }
    fn save(&self) -> Result<(), Error> {
        let buf = rkyv::to_bytes::<Error>(&self.log).unwrap();
        fs::write(PathBuf::from(self.path.clone()), buf).expect("Unable to write file");
        Ok(())
    }
    fn push(&mut self, msg: WALOP) -> Result<(), Error> {
        self.log.push(msg);
        self.save()
    }
}

struct LogServer {
    path: PathBuf,
    wal: WAL,
    sender: tokio::sync::mpsc::Sender<WALOP>,
    receiver: tokio::sync::mpsc::Receiver<WALOP>,
}

impl LogServer {
    fn new(path: PathBuf) -> LogServer {
        // TODO: perf this
        let (tx, rx): (mpsc::Sender<WALOP>, mpsc::Receiver<WALOP>) = mpsc::channel(1000);
        let wal = WAL::open(path.clone());
        LogServer {
            path,
            wal,
            sender: tx,
            receiver: rx,
        }
    }

    async fn listen(&mut self) {
        loop {
            if let Some(msg) = self.receiver.recv().await {
                match self.wal.push(msg) {
                    Ok(_) => {}
                    Err(_) => {}
                }
            }
        }
    }
}

struct Node {
    wal_chan: tokio::sync::mpsc::Sender<WALOP>,
}

impl Node {
    fn new_file(&self) {}
    async fn log(&self, msg: WALOP) -> Result<(), mpsc::error::SendError<WALOP>> {
        self.wal_chan.send(msg).await
    }
    fn delete_file(&self) {}
}

async fn start_server() {
    todo!();
}

#[tokio::main]
async fn main() {
    let mut global_config = FuguConfig {
        dir: "./fugu".into(),
    };
    // register
    let matches = command!()
        .arg(arg!([namespace] "namespace to operate on"))
        .arg(
            arg!(
                -c --config <FILE> "Sets a custom config file"
            )
            .required(false)
            .value_parser(value_parser!(PathBuf)),
        )
        .subcommand(
            Command::new("config")
                .about("edit the fugu configuration")
                .arg(
                    arg!(
                        -p --path "alternate config file path"
                    )
                    .required(false)
                    .value_parser(value_parser!(PathBuf)),
                ),
        )
        .subcommand(
            Command::new("init")
                .about("initializes a fugu instance at the given namespace")
                .arg(arg!([namespace] "namespace to operate on")),
        )
        .subcommand(
            Command::new("status")
                .about("status of the fugu instance at the given namespace")
                .arg(arg!([namespace] "namespace to operate on")),
        )
        .subcommand(
            Command::new("down")
                .about("gracefully shuts down the fugu process unless the force flag is present")
                .arg(arg!(-f --force "forcefully shuts down the fugu server")),
        )
        .subcommand(
            Command::new("up")
                .about("starts the parent fugu process")
                .arg(arg!([namespace] "namespace to operate on"))
                .arg(arg!(-c --config "config for the server").required(false)),
        )
        .get_matches();

    fn cmd_namespace(m: ArgMatches) -> Option<String> {
        m.get_one::<String>("namespace").cloned()
    }

    // Handle global namespace first
    if let Some(namespace) = cmd_namespace(matches.clone()) {
        println!("operating on namespace: {namespace}");
        println!("Nothing to do. Exiting");
        return;
    }

    // check if the $HOME/.fugurc directory has been set
    // check if the config in the $HOME/.fugurc directory is correct
    // set default config

    // if the config is custom, use the correct path
    match matches.get_one::<PathBuf>("config") {
        Some(config_path) => {}
        None => {}
    }

    // Handle subcommands
    match matches.subcommand() {
        Some(("init", sub_matches)) => match cmd_namespace(sub_matches.clone()) {
            Some(namespace) => {
                println!("Initializing fugu at `{namespace}`...");
            }
            None => {
                println!("No namespace provided for init command");
            }
        },
        Some(("status", sub_matches)) => match cmd_namespace(sub_matches.clone()) {
            Some(namespace) => {
                println!("Getting status of fugu node at `{namespace}`...");
            }
            None => {
                println!("No namespace provided for status command");
            }
        },
        Some(("down", sub_matches)) => {
            let force = sub_matches.get_flag("force");
            if force {
                println!("Force shutting down fugu...");
            } else {
                println!("Gracefully shutting down fugu...");
            }
        }
        Some(("up", sub_matches)) => match cmd_namespace(sub_matches.clone()) {
            Some(namespace) => println!("Starting fugu node at `{namespace}`..."),
            None => println!("No namespace provided for up command"),
        },
        Some((cmd, _)) => {
            println!("Unknown command: {cmd}");
        }
        None => {
            println!("No subcommand provided");
        }
    }

    if let Some(namespace) = cmd_namespace(matches.clone()) {
        println!("operating on namespace: {namespace}");
        println!("Nothing to do. Exiting");
        return;
    }
}

use rand::prelude::*;
use aws_sdk_s3 as s3;
use std::fs;
use std::path::PathBuf;
use std::sync::mpsc::SendError;
use std::{collections::HashMap, fmt};
use tokio::sync::mpsc;
// use tokio::time::{sleep, Duration};
use rkyv::{rancor::Error, Archive, Deserialize, Serialize};

use clap::{arg, command, value_parser, ArgAction, ArgMatches, Command, Subcommand};
use sled;

// the following is from
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

struct NodeConfig {}

#[derive(Archive, Debug, Deserialize, Serialize, Clone)]
enum WALOP {
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
enum WALCMD {
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

#[derive(Archive, Deserialize, Serialize)]
struct WAL {
    path: String,
    log: Vec<WALOP>,
}

impl WAL {
    fn open(path: PathBuf) -> WAL {
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

    fn dump(&self) -> Result<String, Error> {
        let mut output = String::new();
        for (i, op) in self.log.iter().enumerate() {
            output.push_str(&format!("{}. {:?}\n", i + 1, op));
        }
        Ok(output)
    }
}

struct Fugu {
    path: PathBuf,
    wal: WAL,
    sender: tokio::sync::mpsc::Sender<WALCMD>,
    receiver: tokio::sync::mpsc::Receiver<WALCMD>,
}

impl Fugu {
    fn new(path: PathBuf) -> Self {
        // TODO: perf this
        let (tx, rx): (mpsc::Sender<WALCMD>, mpsc::Receiver<WALCMD>) = mpsc::channel(1000);
        let wal = WAL::open(path.clone());
        Fugu {
            path,
            wal,
            sender: tx,
            receiver: rx,
        }
    }
    fn get_sender(&self) -> mpsc::Sender<WALCMD> {
        self.sender.clone()
    }
    async fn dump_wal(&self) -> Result<String, Error> {
        Ok(self.wal.dump()?)
    }

    async fn listen(&mut self) {
        loop {
            if let Some(msg) = self.receiver.recv().await {
                match msg {
                    WALCMD::Put { .. } | WALCMD::Delete { .. } | WALCMD::Patch { .. } => {
                        match self.wal.push(msg.into()) {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                    WALCMD::DumpWAL { response } => {
                        if let Ok(dump) = self.dump_wal().await {
                            let _ = response.send(dump);
                        }
                    }
                }
            }
        }
    }

    fn up() {}
    fn down() {}
}

struct Node {
    namespace: String,
    wal_chan: tokio::sync::mpsc::Sender<WALCMD>,
}

impl Node {
    fn new(namespace: String, wal_chan: mpsc::Sender<WALCMD>) -> Self {
        Node {
            namespace,
            wal_chan,
        }
    }
    fn new_file(&self) {}
    async fn log(&self, msg: WALOP) -> Result<(), mpsc::error::SendError<WALCMD>> {
        let msg_clone = msg.clone();
        match self.wal_chan.send(msg.into()).await {
            Ok(_) => {
                println!("{:?}", msg_clone);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
    fn delete_file(&self) {}
}

async fn run_test_scenario() -> Result<(), Box<dyn std::error::Error>> {
    let wal_path = PathBuf::from("./test_wal.bin");
    let mut log_server = Fugu::new(wal_path);
    let sender = log_server.get_sender();

    let server_handle = tokio::spawn(async move {
        log_server.listen().await;
    });

    let mut nodes = Vec::new();
    let mut handles = Vec::new();

    // Create 5 nodes
    for i in 0..5 {
        let node = Node::new(format!("node_{}/", i), sender.clone());
        nodes.push(node);
    }

    // Spawn concurrent tasks for each node
    for (i, node) in nodes.into_iter().enumerate() {
        let handle = tokio::spawn(async move {
            // Each node does random operations
            for j in 1..10 {
                let random = rand::random::<u64>() % 500;
                // Random delay between operations (0-500ms)
                tokio::time::sleep(tokio::time::Duration::from_millis(random)).await;

                // Randomly choose between put and delete
                if rand::random_bool(0.7) {
                    // 70% chance of put, 30% chance of delete
                    let key = format!("node_{}_key_{}", i, j);
                    let value = format!("value_from_node_{}_op_{}", i, j).into_bytes();
                    let _ = node.log(WALOP::Put { key, value }).await;
                } else {
                    // Delete a random previous key
                    let prev_key = format!("node_{}_key_{}", i, rand::random::<u32>() % j);
                    let _ = node.log(WALOP::Delete { key: prev_key }).await;
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all node operations to complete
    for handle in handles {
        handle.await?;
    }

    // Sleep briefly to ensure all operations are processed
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Dump the WAL contents
    println!("\n\n=============\n\n");
    println!("WAL Contents:");
    let (tx, rx) = tokio::sync::oneshot::channel();
    sender.send(WALCMD::DumpWAL { response: tx }).await?;
    println!("{}", rx.await?);

    // Clean up
    server_handle.abort();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // let config = aws_config::load_from_env().await;
    // let client = aws_sdk_s3::Client::new(&config);
    match run_test_scenario().await {
        Ok(_) => println!("test ran successfully!"),
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
    Ok(())
    // let mut global_config = FuguConfig {
    //     dir: "./fugu".into(),
    // };
    // // register
    // let matches = command!()
    //     .arg(arg!([namespace] "namespace to operate on"))
    //     .arg(
    //         arg!(
    //             -c --config <FILE> "Sets a custom config file"
    //         )
    //         .required(false)
    //         .value_parser(value_parser!(PathBuf)),
    //     )
    //     .subcommand(
    //         Command::new("config")
    //             .about("edit the fugu configuration")
    //             .arg(
    //                 arg!(
    //                     -p --path "alternate config file path"
    //                 )
    //                 .required(false)
    //                 .value_parser(value_parser!(PathBuf)),
    //             ),
    //     )
    //     .subcommand(
    //         Command::new("init")
    //             .about("initializes a fugu instance at the given namespace")
    //             .arg(arg!([namespace] "namespace to operate on")),
    //     )
    //     .subcommand(
    //         Command::new("status")
    //             .about("status of the fugu instance at the given namespace")
    //             .arg(arg!([namespace] "namespace to operate on")),
    //     )
    //     .subcommand(
    //         Command::new("down")
    //             .about("gracefully shuts down the fugu process unless the force flag is present")
    //             .arg(arg!(-f --force "forcefully shuts down the fugu server")),
    //     )
    //     .subcommand(
    //         Command::new("up")
    //             .about("starts the parent fugu process")
    //             .arg(arg!([namespace] "namespace to operate on"))
    //             .arg(arg!(-c --config "config for the server").required(false)),
    //     )
    //     .get_matches();

    // fn cmd_namespace(m: ArgMatches) -> Option<String> {
    //     m.get_one::<String>("namespace").cloned()
    // }

    // // Handle global namespace first
    // if let Some(namespace) = cmd_namespace(matches.clone()) {
    //     println!("operating on namespace: {namespace}");
    //     println!("Nothing to do. Exiting");
    //     return;
    // }

    // // check if the $HOME/.fugurc directory has been set
    // // check if the config in the $HOME/.fugurc directory is correct
    // // set default config

    // // if the config is custom, use the correct path
    // match matches.get_one::<PathBuf>("config") {
    //     Some(config_path) => {}
    //     None => {}
    // }

    // // Handle subcommands
    // match matches.subcommand() {
    //     Some(("init", sub_matches)) => match cmd_namespace(sub_matches.clone()) {
    //         Some(namespace) => {
    //             println!("Initializing fugu at `{namespace}`...");
    //         }
    //         None => {
    //             println!("No namespace provided for init command");
    //         }
    //     },
    //     Some(("status", sub_matches)) => match cmd_namespace(sub_matches.clone()) {
    //         Some(namespace) => {
    //             println!("Getting status of fugu node at `{namespace}`...");
    //         }
    //         None => {
    //             println!("No namespace provided for status command");
    //         }
    //     },
    //     Some(("down", sub_matches)) => {
    //         let force = sub_matches.get_flag("force");
    //         if force {
    //             println!("Force shutting down fugu...");
    //         } else {
    //             println!("Gracefully shutting down fugu...");
    //         }
    //     }
    //     Some(("up", sub_matches)) => match cmd_namespace(sub_matches.clone()) {
    //         Some(namespace) => println!("Starting fugu node at `{namespace}`..."),
    //         None => println!("No namespace provided for up command"),
    //     },
    //     Some((cmd, _)) => {
    //         println!("Unknown command: {cmd}");
    //     }
    //     None => {
    //         println!("No subcommand provided");
    //     }
    // }

    // if let Some(namespace) = cmd_namespace(matches.clone()) {
    //     println!("operating on namespace: {namespace}");
    //     println!("Nothing to do. Exiting");
    //     return;
    // }
}

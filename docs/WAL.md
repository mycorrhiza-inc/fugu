# Write-Ahead Log (WAL)

The Write-Ahead Log (WAL) is a critical component of Fugu that ensures data durability. This document covers the WAL architecture, operations, and configuration.

## WAL Overview

The WAL provides:
- Persistence of operations before they're applied to indexes
- Recovery capability in case of crashes
- Atomic operations for consistent state
- Fast, multithreaded append-only operations
- Namespace isolation with a dedicated WAL per namespace

## Architecture

The WAL system consists of:

1. **WAL**: The main WAL manager that maintains separate WALs for each namespace
2. **WALWriter**: Handles writing operations to the WAL file
3. **WALBatch**: Represents a batch of operations to be written together
4. **WALOP**: Represents an individual WAL operation (Put, Delete, Patch)
5. **WALCMD**: Commands sent through the WAL channel

### Directory Structure

Each namespace has its own WAL file:

```
~/.fugu/namespaces/my_namespace/wal.bin
```

## Operations

The WAL supports the following operations:

### Put

Adds or updates a key-value pair:

```rust
WALOP::Put { 
    key: String, 
    value: Vec<u8> 
}
```

### Delete

Removes a key-value pair:

```rust
WALOP::Delete { 
    key: String 
}
```

### Patch

Partially updates an existing key-value pair:

```rust
WALOP::Patch { 
    key: String, 
    value: Vec<u8> 
}
```

## Commands

WAL commands extend operations with namespace information and support additional management functions:

### Data Commands

```rust
// Insert or update a key-value pair
WALCMD::Put {
    key: String,
    value: Vec<u8>,
    namespace: String,
}

// Delete a key-value pair
WALCMD::Delete {
    key: String,
    namespace: String,
}

// Partially update a key-value pair
WALCMD::Patch {
    key: String,
    value: Vec<u8>,
    namespace: String,
}
```

### Management Commands

```rust
// Dump the WAL contents for debugging
WALCMD::DumpWAL {
    response: oneshot::Sender<String>,
    namespace: Option<String>,
}

// Flush a namespace's WAL to disk
WALCMD::FlushWAL {
    namespace: String,
    response: Option<oneshot::Sender<io::Result<()>>>,
}
```

## WAL Lifecycle

1. **Initialization**: When a node or server starts, the WAL system is initialized
2. **Operation Processing**: Commands are sent through a channel to the WAL system
3. **Batching**: Operations are batched for efficient disk writing
4. **Flushing**: Batches are periodically flushed to disk
5. **Recovery**: On startup, the WAL is replayed to restore system state if needed
6. **Shutdown**: On shutdown, all pending operations are flushed to disk

## Performance Optimization

The WAL includes several performance optimizations:

### Batching

Operations are batched together to minimize disk I/O:

```rust
// Size of buffer for batching WAL operations (bytes)
const BUFFER_SIZE: usize = 1024 * 1024; // 1MB
```

### Concurrent Writers

Multiple writers can append to the WAL concurrently:

```rust
// Maximum number of concurrent writers to the WAL
const MAX_CONCURRENT_WRITERS: usize = 8;
```

### Periodic Flushing

A background task periodically flushes the WAL to disk:

```rust
// How frequently to flush buffers (milliseconds)
const FLUSH_INTERVAL_MS: u64 = 100;
```

### Recent Operations Cache

Recent operations are kept in memory for efficient access:

```rust
// Maximum number of recent operations to keep in memory per namespace
max_recent_ops: usize = 1000;
```

## Advanced Features

### Namespace Isolation

Each namespace has its own WAL file, which provides:
- Isolation between namespaces
- Independent recovery
- Optimized performance
- Reduced contention

### Background Flush

A background task periodically flushes all WALs:

```rust
async fn start_background_flush(&self) {
    // ...flush code...
}
```

### Serialization

The WAL uses the rkyv serialization framework for efficient binary serialization:

```rust
fn serialize(&self) -> Result<Vec<u8>, io::Error> {
    match rkyv::to_bytes::<rkyv::rancor::Panic>(&self.operations) {
        Ok(aligned) => Ok(aligned.to_vec()),
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("Serialization error: {:?}", e)))
    }
}
```

## API Usage

### Sending WAL Commands

```rust
// Get the WAL sender
let wal_sender = server.get_wal_sender();

// Create a PUT command
let cmd = WALCMD::Put { 
    key: "example_key".to_string(), 
    value: b"example_value".to_vec(),
    namespace: "my_namespace".to_string()
};

// Send the command
wal_sender.send(cmd).await?;
```

### Flushing a Namespace

```rust
// Create a channel for the response
let (tx, rx) = tokio::sync::oneshot::channel();

// Create a flush command
let cmd = WALCMD::FlushWAL { 
    namespace: "my_namespace".to_string(),
    response: Some(tx),
};

// Send the command
wal_sender.send(cmd).await?;

// Wait for the flush to complete
let result = rx.await?;
```

### Dumping WAL Contents

```rust
// Create a channel for the response
let (tx, rx) = tokio::sync::oneshot::channel();

// Create a dump command
let cmd = WALCMD::DumpWAL {
    response: tx,
    namespace: Some("my_namespace".to_string()),
};

// Send the command
wal_sender.send(cmd).await?;

// Wait for the dump response
let dump = rx.await?;
println!("WAL contents: {}", dump);
```

## Recovery Process

On startup, the WAL is automatically read and replayed to restore the system state:

1. Each namespace's WAL file is read
2. Operations are deserialized
3. Operations are applied to the corresponding namespace index
4. The system is brought to a consistent state

This ensures data durability even in the event of unexpected shutdowns.
# fugu CLI Tool

A CLI tool for interacting with the fugu server, providing CRUD operations for namespaces.

## Installation

Build the project with Cargo:

```bash
cargo build --release
```

## Usage

The CLI can be used to interact with namespaces on a fugu server:

```bash
# Using default server (http://localhost:3000)
./target/release/fugu namespace list

# Using custom server
./target/release/fugu --server http://your-server:3301 namespace list
```

### Namespace Commands

#### List all namespaces

```bash
./target/release/fugu namespace list
```

#### Add a namespace

```bash
./target/release/fugu namespace add my-namespace
```

#### Delete a namespace

```bash
./target/release/fugu namespace delete my-namespace
```

#### Get filters for a namespace

```bash
./target/release/fugu namespace filters my-namespace
```

#### Search within a namespace

```bash
./target/release/fugu namespace search my-namespace --query "search term"
```

#### Add a file to a namespace

```bash
./target/release/fugu namespace add-file my-namespace --file-name example.txt --content "File content goes here"
```

## Server Mode

Without CLI arguments, the application runs as a server:

```bash
./target/release/fugu
```

This starts the HTTP server on port 3301 and the compactor service.

## API Endpoints

- `GET /health` - Health check endpoint
- `POST /search` - Search all namespaces
- `POST /search/{namespace}` - Search in a specific namespace
- `GET /namespaces` - List all namespaces
- `GET /filters/{namespace}` - List filters for a specific namespace
- `POST /add/{namespace}` - Add a file to a namespace
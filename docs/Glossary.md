# Fugu Glossary

This glossary defines key terms and concepts used throughout the Fugu documentation.

## Core Concepts

### BM25
A ranking function used by search engines to estimate the relevance of documents to a search query. Fugu uses BM25 for scoring search results.

### ConfigManager
A component that manages file paths and directory structure for Fugu, ensuring proper organization and accessibility.

### Document
A file or piece of content that is indexed and made searchable in Fugu.

### Document ID
A unique identifier for a document in the index, typically derived from the filename.

### FuguServer
The main server component that manages the lifecycle of the search engine, including handling the WAL and maintaining background tasks.

### gRPC
A high-performance RPC (Remote Procedure Call) framework used by Fugu for client-server communication.

### Index
The data structure that enables efficient text search by mapping terms to documents and positions.

### Inverted Index
A data structure that maps content (terms) to their locations in documents, rather than documents to their content.

### Namespace
A logical grouping of documents that provides isolation and multi-tenant support. Each namespace has its own independent index.

### Node
A component that represents a specific namespace and manages operations within that namespace.

### Position
The location of a term within a document, used for phrase matching and relevance scoring.

### Relevance Score
A numerical value that indicates how well a document matches a search query, calculated using BM25 or TF-IDF.

### Term
A word or token extracted from documents during indexing, the basic unit of search.

### Token
A data structure representing a term, its document ID, and position within the document.

### Tokenizer
A component that breaks text into tokens (terms) during indexing and searching.

### WAL (Write-Ahead Log)
A durability mechanism that records operations before they are applied to the index, ensuring data integrity even if the system crashes.

## Operations

### Indexing
The process of analyzing documents and creating data structures that enable fast retrieval during search operations.

### Search
The process of finding documents that match a user's query by examining the index.

### Delete
The operation of removing a document from the index.

### Flush
The operation of writing in-memory data to disk to ensure durability.

### Streaming
A method of processing large files by breaking them into chunks to avoid loading the entire file into memory.

## Technical Terms

### Batching
Grouping multiple operations together for more efficient processing.

### CRDT (Conflict-free Replicated Data Type)
A data structure that can be replicated across multiple computers in a network, where replicas can be updated independently and concurrently without coordination between them, and it is always mathematically possible to resolve inconsistencies.

### Namespace Isolation
The separation of data and operations between different namespaces to prevent interference.

### Parallel Processing
Executing multiple operations concurrently to improve performance, especially for large files.

### Term Frequency (TF)
The number of times a term appears in a document, used in relevance scoring.

### Inverse Document Frequency (IDF)
A measure of how common or rare a term is across all documents, used in relevance scoring.

### TF-IDF
A numerical statistic that reflects how important a word is to a document in a collection or corpus, used as a fallback scoring mechanism for case-insensitive search.

## Components

### IndexRequest
A gRPC message for indexing a document.

### SearchRequest
A gRPC message for searching the index.

### DeleteRequest
A gRPC message for deleting a document from the index.

### StreamIndexChunk
A part of a file being streamed for indexing, used for handling large files efficiently.

### SearchResult
A document that matches a search query, including its path, relevance score, and snippet.

### Snippet
A small extract from a document showing the context around matching terms.

### WALOP
An operation recorded in the Write-Ahead Log (Put, Delete, or Patch).

### WALCMD
A command sent through the WAL channel, including namespace information.

## Performance Terms

### Hot Search
Search operations performed when the index is already loaded in memory.

### Cold Search
Search operations performed when the index must be loaded from disk first.

### Throughput
The number of operations (indexing, search, delete) that can be processed per unit of time.

### Latency
The time taken to complete a single operation.

### p50, p90, p99
Percentile metrics for performance, indicating the latency at the 50th, 90th, and 99th percentiles.
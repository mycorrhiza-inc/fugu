# Searching with Fugu

Fugu provides powerful text search capabilities through its inverted index implementation. This document covers search functionality, algorithms, and best practices.

## Search Basics

Fugu uses an inverted index to enable fast text search. The search process involves:

1. **Query Parsing**: Breaking the query into tokens
2. **Term Lookup**: Finding documents containing the query terms
3. **Relevance Scoring**: Ranking results using the BM25 algorithm
4. **Result Formatting**: Returning documents with relevance scores and snippets

## How to Search

### Using the Command Line

To search in the default namespace:

```bash
cargo run -- search "your search query" --addr http://localhost:50051
```

To search in a specific namespace:

```bash
cargo run -- search --namespace my_namespace --limit 20 "your search query"
```

Options:
- `--namespace`: The namespace to search in (optional, defaults to "default")
- `--limit`: Maximum number of results to return (default: 10)
- `--offset`: Number of results to skip for pagination (default: 0)
- `--addr`: Server address (default: http://127.0.0.1:50051)

### Using the gRPC API

For programmatic access, use the gRPC client:

```rust
let mut client = NamespaceClient::connect("http://localhost:50051").await?;
let response = client.search_with_metadata(
    "your search query".to_string(),
    10,  // limit
    0,   // offset
    metadata
).await?;
```

## Search Results

Search results include:

- **Document ID**: The identifier of the matching document
- **Relevance Score**: A number indicating how well the document matches the query
- **Term Matches**: Positions where query terms appear in the document
- **Snippets**: Context around the matching terms (when available)

## Search Algorithm

Fugu implements BM25 scoring for relevance ranking:

### BM25 Formula

```
score(D, Q) = ∑(IDF(t) · TF(t, D) · (k₁ + 1)) / (TF(t, D) + k₁ · (1 - b + b · |D| / avgdl))
```

Where:
- D is a document
- Q is the query
- TF(t, D) is the term frequency of term t in document D
- IDF(t) is the inverse document frequency of term t
- |D| is the document length
- avgdl is the average document length
- k₁ and b are tuning parameters (defaults: k₁=1.2, b=0.75)

### TF-IDF Fallback

For case-insensitive search, Fugu falls back to a TF-IDF scoring approach:

```
score(D, Q) = ∑(TF(t, D) · log(N / DF(t)))
```

Where:
- N is the total number of documents
- DF(t) is the number of documents containing term t

## Performance Optimization

Fugu collects detailed search metrics to help optimize performance:

- **Query Parsing Time**: Time taken to parse and prepare the query
- **Retrieval Time**: Time taken to find matching documents
- **Scoring Time**: Time taken to rank results
- **Total Time**: Overall search time
- **Documents Searched**: Number of documents examined
- **Documents Matched**: Number of documents that matched the query

## Advanced Search Features

### Case-Insensitive Search

All searches in Fugu are case-insensitive by default. Terms are normalized to lowercase during both indexing and searching.

### Result Pagination

Use the `--limit` and `--offset` parameters to implement pagination:

```bash
# First page
cargo run -- search --limit 10 --offset 0 "your query"

# Second page
cargo run -- search --limit 10 --offset 10 "your query"
```

### Search Performance

For optimal search performance:

1. Use specific, distinctive terms in your queries
2. Limit results to what you need (use sensible limit values)
3. Choose namespaces with well-defined boundaries
4. Consider the size of your index (larger indices may require more resources)

## Vector Search

Fugu includes experimental vector search capabilities (currently limited):

```bash
cargo run -- vector-search --dim 128 [vector values...]
```

This will be expanded in future versions to support semantic search and embeddings.
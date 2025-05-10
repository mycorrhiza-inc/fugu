# Nugu Search API Documentation

This document describes the search API endpoints available in the Nugu search server.

## Query Endpoints

### Simple Text Query (GET)

Perform a simple text search.

```
GET /query?q=your search query&limit=10
```

Parameters:
- `q`: (Required) The search query text.
- `limit`: (Optional) Maximum number of results to return.

Example:
```
GET /query?q=rust programming&limit=5
```

### URL-Encoded Path Query (GET)

Perform a search using a URL-encoded query in the path.

```
GET /query/{encoded_query}?limit=10
```

Parameters:
- `encoded_query`: (Required) URL-encoded search query text.
- `limit`: (Optional) Maximum number of results to return.

Example:
```
GET /query/rust%20programming?limit=5
```

### JSON Query (POST)

Perform a search with additional options using JSON.

```
POST /query
```

Request body:
```json
{
  "query": "search text",
  "top_k": 10,
  "filters": [...],
  "boost": [...]
}
```

Parameters:
- `query`: (Required) The search query text.
- `top_k`: (Optional) Maximum number of results to return.
- `filters`: (Optional) Array of filter expressions.
- `boost`: (Optional) Array of boost criteria.

### Advanced Query (POST)

Perform a search with custom configuration options.

```
POST /query/advanced
```

Request body:
```json
{
  "query": "search text",
  "limit": 10,
  "config": {
    "highlight_snippets": true,
    "min_score_threshold": 0.01,
    "bm25_k1": 1.2,
    "bm25_b": 0.75
  }
}
```

Parameters:
- `query`: (Required) The search query text.
- `limit`: (Optional) Maximum number of results to return.
- `config`: (Optional) Custom configuration options.

## Response Format

All query endpoints return responses in the following format:

```json
{
  "hits": [
    {
      "document_id": "doc1",
      "score": 0.75,
      "highlights": ["This is a **match** in context"],
      "metadata": { ... }
    },
    ...
  ],
  "total_hits": 42,
  "took_ms": 5
}
```

Fields:
- `hits`: Array of matching documents.
- `total_hits`: Total number of documents that matched the query.
- `took_ms`: Time taken to execute the query in milliseconds.

Each hit contains:
- `document_id`: Unique identifier for the document.
- `score`: Relevance score (higher is better).
- `highlights`: Optional snippets with highlighted matches.
- `metadata`: Optional document metadata.

## Query Syntax

### Basic Search
- Simple terms: `rust programming`
- Phrase search: `"rust programming"`

### Operators
- AND: `rust AND programming`
- OR: `rust OR python`
- NOT: `rust NOT beginner`

### Term Boosting
- Boost terms: `rust^2 programming`

## Example Queries

### Simple Text Query

```
GET /query?q=rust programming
```

### JSON Query with Filter

```
POST /query
Content-Type: application/json

{
  "query": "rust programming",
  "top_k": 5,
  "filters": [
    {
      "type": "term",
      "field": "category",
      "value": "programming"
    }
  ]
}
```

### Advanced Query with Custom Configuration

```
POST /query/advanced
Content-Type: application/json

{
  "query": "rust programming",
  "limit": 10,
  "config": {
    "highlight_snippets": true,
    "min_score_threshold": 0.05,
    "bm25_k1": 1.5,
    "bm25_b": 0.8
  }
}
```
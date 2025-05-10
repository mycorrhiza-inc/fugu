#!/usr/bin/env python3
"""
Generate large test documents with random English words for benchmarking.
"""

import random
import json
import os
import sys
from pathlib import Path

# A list of common English words to use for test data
COMMON_WORDS = [
    "the", "be", "to", "of", "and", "a", "in", "that", "have", "I", "it", "for", "not", "on", "with",
    "he", "as", "you", "do", "at", "this", "but", "his", "by", "from", "they", "we", "say", "her", "she",
    "or", "an", "will", "my", "one", "all", "would", "there", "their", "what", "so", "up", "out", "if",
    "about", "who", "get", "which", "go", "me", "when", "make", "can", "like", "time", "no", "just", "him",
    "know", "take", "people", "into", "year", "your", "good", "some", "could", "them", "see", "other", "than",
    "then", "now", "look", "only", "come", "its", "over", "think", "also", "back", "after", "use", "two",
    "how", "our", "work", "first", "well", "way", "even", "new", "want", "because", "any", "these", "give",
    "day", "most", "us", "information", "document", "large", "benchmark", "test", "data", "query", "index",
    "search", "result", "performance", "analysis", "measure", "speed", "fast", "slow", "optimize", "improve",
    "enhance", "increase", "decrease", "compare", "evaluation", "metric", "statistic", "calculate", "compute",
    "process", "database", "storage", "retrieve", "fetch", "store", "save", "update", "delete", "insert",
    "create", "read", "write", "modify", "change", "text", "content", "paragraph", "sentence", "word"
]

# Add some domain-specific words related to search and databases
DOMAIN_WORDS = [
    "database", "index", "query", "search", "retrieval", "document", "record", "field", "term", "token",
    "analyzer", "stemming", "lemmatization", "tokenization", "inverted", "posting", "list", "relevance",
    "ranking", "score", "match", "exact", "fuzzy", "proximity", "phrase", "boolean", "wildcard", "regex",
    "facet", "filter", "sort", "pagination", "highlight", "suggestion", "autocomplete", "spellcheck",
    "synonym", "stopword", "normalization", "vectorize", "embedding", "semantic", "similarity", "cosine",
    "distance", "euclidean", "manhattan", "jaccard", "precision", "recall", "f1", "accuracy", "throughput",
    "latency", "benchmark", "performance", "scalability", "sharding", "replication", "consistency", "durability",
    "availability", "partition", "cluster", "node", "transaction", "concurrent", "parallel", "sequential"
]

ALL_WORDS = COMMON_WORDS + DOMAIN_WORDS

def generate_random_text(target_size_kb):
    """Generate random text with target size in KB."""
    target_size_bytes = target_size_kb * 1024
    text = []
    
    # Generate paragraphs until we reach the target size
    current_size = 0
    while current_size < target_size_bytes:
        # Generate a paragraph (50-200 words)
        paragraph_length = random.randint(50, 200)
        paragraph = []
        
        for _ in range(paragraph_length):
            paragraph.append(random.choice(ALL_WORDS))
        
        formatted_paragraph = " ".join(paragraph)
        text.append(formatted_paragraph)
        current_size = len("\n\n".join(text).encode('utf-8'))
    
    return "\n\n".join(text)

def generate_record(doc_id, size_kb):
    """Generate a record with the specified size."""
    text = generate_random_text(size_kb)
    
    # Create record with metadata
    record = {
        "id": doc_id,
        "text": text,
        "metadata": {
            "benchmark": True,
            "size_kb": size_kb,
            "word_count": len(text.split()),
            "timestamp": {"$currentTime": True},
            "properties": {
                "type": "benchmark_large",
                "category": "large_document_test"
            }
        }
    }
    
    return record

def main():
    # Create output directory
    output_dir = Path("./benchmark_data")
    output_dir.mkdir(exist_ok=True)
    
    # Get parameters from command line
    doc_size_kb = 500  # Default 500KB per document
    num_docs = 10      # Default 10 documents (5MB total)
    
    if len(sys.argv) > 1:
        doc_size_kb = int(sys.argv[1])
    if len(sys.argv) > 2:
        num_docs = int(sys.argv[2])
    
    print(f"Generating {num_docs} documents of {doc_size_kb}KB each")
    total_size_mb = (doc_size_kb * num_docs) / 1024
    print(f"Total data size: {total_size_mb:.2f}MB")
    
    for i in range(num_docs):
        doc_id = f"large_doc_{i+1}"
        record = generate_record(doc_id, doc_size_kb)
        
        # Save to JSON file
        output_path = output_dir / f"{doc_id}.json"
        with open(output_path, 'w') as f:
            json.dump(record, f, indent=2)
        
        actual_size_kb = os.path.getsize(output_path) / 1024
        print(f"Generated {doc_id}: {actual_size_kb:.2f}KB")
    
    print(f"Done! Generated {num_docs} documents in {output_dir}")

if __name__ == "__main__":
    main()
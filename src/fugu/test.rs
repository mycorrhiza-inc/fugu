#[cfg(test)]
mod tests {
    use crate::cmd::commands::AddCommand;
    use clap::Parser;
    use crate::fugu::index::{InvertedIndex, Token, WhitespaceTokenizer};
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    use std::time::Duration;
    use tempfile::tempdir;

    fn tokenize(term: &str, docid: &str, position: u64) -> Token {
        Token {
            term: term.to_string(),
            doc_id: docid.to_string(),
            position,
        }
    }
    
    // Helper function to create a temporary markdown file with content
    fn create_markdown_file(dir: &Path, name: &str, content: &str) -> String {
        let file_path = dir.join(format!("{}.md", name));
        let mut file = File::create(&file_path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file_path.to_str().unwrap().to_string()
    }

    // Tokenize a markdown file into individual terms
    fn tokenize_markdown_file(file_content: &str, doc_id: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut position: u64 = 0;

        // Split content by whitespace and punctuation
        for line in file_content.lines() {
            // Skip empty lines
            if line.trim().is_empty() {
                continue;
            }

            // Process each word
            let words = line
                .split(|c: char| c.is_whitespace() || c.is_ascii_punctuation())
                .filter(|s| !s.is_empty());

            for word in words {
                position += 1;
                // Convert to lowercase to ensure case-insensitive search
                let term = word.to_lowercase();
                if !term.is_empty() {
                    tokens.push(tokenize(&term, doc_id, position));
                }
            }
        }

        tokens
    }

    #[tokio::test]
    async fn test_index_markdown_files() {
        // Create temporary directory for test files
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path();

        // Create test markdown files with their content
        let file1_content = "# Hello world\n\nThis is a test document with some keywords.";
        let file2_content =
            "# Another document\n\nThis document contains different keywords and test terms.";
        let file3_content = "# Third file\n\nMore content with test and world keywords.";
        let file4_content = "# Fourth document\n\nFinal test document with unique words.";
        let file5_content = "# Empty document\n\nThis has no special keywords.";

        let _file1_path = create_markdown_file(temp_path, "file1", file1_content);
        let _file2_path = create_markdown_file(temp_path, "file2", file2_content);
        let _file3_path = create_markdown_file(temp_path, "file3", file3_content);
        let _file4_path = create_markdown_file(temp_path, "file4", file4_content);
        let _file5_path = create_markdown_file(temp_path, "file5", file5_content);

        // Create a temporary directory for the index
        let index_dir = tempdir().unwrap();
        let index_path = index_dir.path().join("index").to_str().unwrap().to_string();

        // Create the inverted index
        let index = InvertedIndex::new(&index_path).await;

        // Tokenize and add terms from all files
        let files = [
            ("file1", file1_content),
            ("file2", file2_content),
            ("file3", file3_content),
            ("file4", file4_content),
            ("file5", file5_content),
        ];

        for (doc_id, content) in files.iter() {
            let tokens = tokenize_markdown_file(content, doc_id);
            for token in tokens {
                index.add_term(token).await.unwrap();
            }
        }

        // Test searching for terms

        // Test 1: Search for a term that appears in multiple documents
        let result = index.search("test").await.unwrap();
        assert!(result.is_some());
        let term_index = result.unwrap();
        assert_eq!(term_index.term, "test");
        assert!(term_index.term_frequency > 0);
        assert!(term_index.doc_ids.len() >= 3);
        assert!(term_index.doc_ids.contains_key("file1"));
        assert!(term_index.doc_ids.contains_key("file2"));
        assert!(term_index.doc_ids.contains_key("file3"));
        assert!(term_index.doc_ids.contains_key("file4"));

        // Test 2: Search for a term that appears in specific documents
        let result = index.search("world").await.unwrap();
        assert!(result.is_some());
        let term_index = result.unwrap();
        assert_eq!(term_index.term, "world");
        assert!(term_index.term_frequency > 0);
        assert!(term_index.doc_ids.len() >= 2);
        assert!(term_index.doc_ids.contains_key("file1"));
        assert!(term_index.doc_ids.contains_key("file3"));

        // Test 3: Search for a term that doesn't exist
        let result = index.search("nonexistent").await.unwrap();
        assert!(result.is_none());

        // Test 4: Search for a term that appears multiple times in the same document
        let result = index.search("document").await.unwrap();
        assert!(result.is_some());
        let term_index = result.unwrap();
        assert_eq!(term_index.term, "document");
        assert!(term_index.doc_ids.len() >= 4);
        assert!(term_index.doc_ids.contains_key("file1"));
        assert!(term_index.doc_ids.contains_key("file2"));
        assert!(term_index.doc_ids.contains_key("file4"));
        assert!(term_index.doc_ids.contains_key("file5"));
        assert!(term_index.doc_ids.get("file2").unwrap().len() >= 2); // Should have multiple positions in file2

        // Test removing terms

        // Test 5: Remove a term from a document
        index.remove_term("test", "file1").await.unwrap();
        let result = index.search("test").await.unwrap();
        assert!(result.is_some());
        let term_index = result.unwrap();
        assert!(term_index.doc_ids.len() >= 3);
        assert!(!term_index.doc_ids.contains_key("file1"));
        assert!(term_index.doc_ids.contains_key("file2"));
        assert!(term_index.doc_ids.contains_key("file3"));
        assert!(term_index.doc_ids.contains_key("file4"));

        // Test 6: Remove a term from all documents
        index.remove_term("world", "file1").await.unwrap();
        index.remove_term("world", "file3").await.unwrap();
        let result = index.search("world").await.unwrap();
        assert!(result.is_none()); // Term should be completely removed

        // Test 7: Remove a term that appears in multiple locations in a document
        index.remove_term("document", "file4").await.unwrap();
        let result = index.search("document").await.unwrap();
        assert!(result.is_some());
        let term_index = result.unwrap();
        assert!(!term_index.doc_ids.contains_key("file4"));

        // Clean up temporary files and directories
        temp_dir.close().unwrap();
        index_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_search_text() {
        // Create temporary directory for test files
        let temp_dir = tempdir().unwrap();
        let _temp_path = temp_dir.path();

        // Create a temporary directory for the index
        let index_dir = tempdir().unwrap();
        let index_path = index_dir.path().join("index").to_str().unwrap().to_string();

        // Create the inverted index
        let index = InvertedIndex::new(&index_path).await;

        // Create a tokenizer
        let tokenizer = WhitespaceTokenizer;

        // Index some test documents
        let documents = [
            ("doc1", "The quick brown fox jumps over the lazy dog"),
            ("doc2", "A quick brown dog chases the fox"),
            ("doc3", "The lazy fox sleeps under the tree"),
            ("doc4", "A completely different document with unique terms"),
        ];

        for (doc_id, content) in documents.iter() {
            index
                .index_document(doc_id, content, &tokenizer)
                .await
                .unwrap();
        }

        // Test 1: Search for a single term
        let results = index.search_text("fox", &tokenizer).await.unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.iter().any(|r| r.doc_id == "doc1"));
        assert!(results.iter().any(|r| r.doc_id == "doc2"));
        assert!(results.iter().any(|r| r.doc_id == "doc3"));

        // Test 2: Search for multiple terms
        let results = index.search_text("quick brown", &tokenizer).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|r| r.doc_id == "doc1"));
        assert!(results.iter().any(|r| r.doc_id == "doc2"));

        // Ensure the most relevant document is ranked first (should be doc1 or doc2 since they have both terms)
        let first_result = &results[0];
        assert!(first_result.doc_id == "doc1" || first_result.doc_id == "doc2");

        // Test 3: Search for a term not in any document
        let results = index.search_text("nonexistent", &tokenizer).await.unwrap();
        assert_eq!(results.len(), 0);

        // Test 4: Search combining common and uncommon terms
        let results = index.search_text("fox unique", &tokenizer).await.unwrap();
        assert_eq!(results.len(), 4); // Should find all docs

        // Check term matches
        let doc4_result = results.iter().find(|r| r.doc_id == "doc4").unwrap();
        assert!(doc4_result.term_matches.contains_key("unique"));
        assert!(!doc4_result.term_matches.contains_key("fox"));

        let doc1_result = results.iter().find(|r| r.doc_id == "doc1").unwrap();
        assert!(!doc1_result.term_matches.contains_key("unique"));
        assert!(doc1_result.term_matches.contains_key("fox"));

        // Test 5: Empty query
        let results = index.search_text("", &tokenizer).await.unwrap();
        assert_eq!(results.len(), 0);

        // Clean up
        temp_dir.close().unwrap();
        index_dir.close().unwrap();
    }

    // Function to collect and calculate percentiles
    #[allow(dead_code)]
    fn calculate_percentiles(
        durations: &mut Vec<Duration>,
    ) -> (Duration, Duration, Duration, Duration) {
        durations.sort();
        let len = durations.len();
        let p10_idx = (len as f64 * 0.1) as usize;
        let p50_idx = (len as f64 * 0.5) as usize;
        let p90_idx = (len as f64 * 0.9) as usize;
        let p99_idx = (len as f64 * 0.99) as usize;

        (
            durations[p10_idx],
            durations[p50_idx],
            durations[p90_idx],
            durations[p99_idx],
        )
    }
    
    // Function to save performance data to CSV
    #[cfg(feature = "performance-tests")]
    fn save_performance_csv(test_name: &str, durations: &[Duration]) -> std::io::Result<()> {
        use std::fs::File;
        use std::io::Write;
        use std::path::Path;
        
        // Create the data directory if it doesn't exist
        let data_dir = Path::new("tests/data");
        if !data_dir.exists() {
            std::fs::create_dir_all(data_dir)?;
        }
        
        // Create CSV file
        let file_path = data_dir.join(format!("{}.csv", test_name));
        let mut file = File::create(file_path)?;
        
        // Write header
        writeln!(file, "operation,duration_us")?;
        
        // Write each measurement
        for (i, duration) in durations.iter().enumerate() {
            writeln!(file, "{},{}", i, duration.as_micros())?;
        }
        
        Ok(())
    }

    // Helper function to create performance test setup
    #[cfg(feature = "performance-tests")]
    async fn setup_performance_test() -> (
        InvertedIndex,
        WhitespaceTokenizer,
        Vec<(String, String)>,
        Vec<String>,
        tempfile::TempDir,
    ) {
        use rand;
        use rand::{rngs::ThreadRng, seq::SliceRandom, Rng};

        // Create a temporary directory for the index
        let index_dir = tempdir().unwrap();
        let index_path = index_dir
            .path()
            .join("perf_index")
            .to_str()
            .unwrap()
            .to_string();

        // Create the inverted index
        let index = InvertedIndex::new(&index_path).await;

        // Create a tokenizer
        let tokenizer = WhitespaceTokenizer;

        // Sample words for generating test data
        let words = vec![
            "apple",
            "banana",
            "cherry",
            "date",
            "elderberry",
            "fig",
            "grape",
            "honeydew",
            "kiwi",
            "lemon",
            "mango",
            "nectarine",
            "orange",
            "pear",
            "quince",
            "raspberry",
            "strawberry",
            "tangerine",
            "watermelon",
            "zucchini",
            "the",
            "a",
            "an",
            "and",
            "but",
            "or",
            "for",
            "nor",
            "yet",
            "so",
            "about",
            "above",
            "across",
            "after",
            "against",
            "along",
            "among",
            "around",
            "at",
            "before",
            "behind",
            "below",
            "beneath",
            "beside",
            "between",
            "beyond",
            "by",
            "down",
            "during",
            "except",
            "for",
            "from",
            "in",
            "inside",
            "into",
            "like",
            "near",
            "of",
            "off",
            "on",
            "onto",
            "out",
            "outside",
            "over",
            "past",
            "since",
            "through",
            "throughout",
            "to",
            "toward",
            "under",
            "underneath",
            "until",
            "up",
            "upon",
            "with",
            "within",
            "without",
        ];
        let sample_words: Vec<String> = words.iter().map(|s| s.to_string()).collect();

        // Number of documents to test
        let num_docs = 100;

        // Generate random documents
        let mut docs = Vec::new();

        for i in 0..num_docs {
            let doc_id = format!("doc_{}", i);
            let doc_length = rand::random_range(10..100);
            let mut content = String::new();

            for _ in 0..doc_length {
                let word = &sample_words[rand::random_range(0..100) % sample_words.len()];
                content.push_str(word.as_str());
                content.push(' ');
            }

            docs.push((doc_id, content));
        }

        (index, tokenizer, docs, sample_words, index_dir)
    }

    #[tokio::test]
    #[cfg(feature = "performance-tests")]
    async fn test_insert_performance() {
        use std::time::{Duration, Instant};

        // Setup the test environment
        let (index, tokenizer, docs, _, index_dir) = setup_performance_test().await;

        let operations_per_test = 1000;
        let _rng = rand::rng();

        println!("Testing insert performance...");
        let mut insert_durations = Vec::with_capacity(operations_per_test);

        for _ in 0..operations_per_test {
            let mut v = vec![];
            for _ in 0..2 {
                v.push(rand::random_range(0..100) % docs.len())
            }
            let (doc_id, content) = &docs[rand::random_range(0..100) % docs.len()];

            let start = Instant::now();
            index
                .index_document(doc_id.as_str(), content.as_str(), &tokenizer)
                .await;
            let duration = start.elapsed();

            insert_durations.push(duration);
        }

        // Save raw performance data to CSV
        if let Err(e) = save_performance_csv("insert_performance", &insert_durations) {
            eprintln!("Failed to save insert performance data to CSV: {}", e);
        }

        let (p10, p50, p90, p99) = calculate_percentiles(&mut insert_durations);
        println!(
            "Insert performance (μs): p10={}, p50={}, p90={}, p99={}",
            p10.as_micros(),
            p50.as_micros(),
            p90.as_micros(),
            p99.as_micros()
        );

        // Clean up
        index_dir.close().unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "performance-tests")]
    async fn test_search_performance() {
        use std::time::{Duration, Instant};

        // Setup the test environment
        let (index, tokenizer, docs, sample_words, index_dir) = setup_performance_test().await;

        let operations_per_test = 1000;
        let _rng = rand::rng();

        // First index documents for searching
        for (doc_id, content) in &docs {
            index
                .index_document(doc_id.as_str(), content.as_str(), &tokenizer)
                .await;
        }

        println!("Testing search performance...");
        let mut search_durations = Vec::with_capacity(operations_per_test);

        for _ in 0..operations_per_test {
            let term = &sample_words[rand::random_range(0..100) % sample_words.len()];

            let start = Instant::now();
            let _ = index.search(term.as_str()).await.unwrap();
            let duration = start.elapsed();

            search_durations.push(duration);
        }

        // Save raw performance data to CSV
        if let Err(e) = save_performance_csv("search_performance", &search_durations) {
            eprintln!("Failed to save search performance data to CSV: {}", e);
        }

        let (p10, p50, p90, p99) = calculate_percentiles(&mut search_durations);
        println!(
            "Search performance (μs): p10={}, p50={}, p90={}, p99={}",
            p10.as_micros(),
            p50.as_micros(),
            p90.as_micros(),
            p99.as_micros()
        );

        // Clean up
        index_dir.close().unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "performance-tests")]
    async fn test_text_search_performance() {
        use std::time::{Duration, Instant};

        // Setup the test environment
        let (index, tokenizer, docs, sample_words, index_dir) = setup_performance_test().await;

        let operations_per_test = 1000;

        // First index documents for searching
        for (doc_id, content) in &docs {
            index
                .index_document(doc_id.as_str(), content.as_str(), &tokenizer)
                .await;
        }

        println!("Testing text search performance...");
        let mut text_search_durations = Vec::with_capacity(operations_per_test);

        for _ in 0..operations_per_test {
            // Generate a query with 1-3 random words
            let query_length = rand::random_range(1..=3);
            let mut query = String::new();

            for i in 0..query_length {
                if i > 0 {
                    query.push(' ');
                }
                query.push_str(&sample_words[rand::random_range(0..100) % sample_words.len()]);
            }

            let start = Instant::now();
            let _ = index.search_text(&query, &tokenizer).await.unwrap();
            let duration = start.elapsed();

            text_search_durations.push(duration);
        }

        // Save raw performance data to CSV
        if let Err(e) = save_performance_csv("text_search_performance", &text_search_durations) {
            eprintln!("Failed to save text search performance data to CSV: {}", e);
        }

        let (p10, p50, p90, p99) = calculate_percentiles(&mut text_search_durations);
        println!(
            "Text search performance (μs): p10={}, p50={}, p90={}, p99={}",
            p10.as_micros(),
            p50.as_micros(),
            p90.as_micros(),
            p99.as_micros()
        );

        // Clean up
        index_dir.close().unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "performance-tests")]
    async fn test_delete_performance() {
        use std::time::{Duration, Instant};

        // Setup the test environment
        let (index, tokenizer, docs, sample_words, index_dir) = setup_performance_test().await;

        let operations_per_test = 1000;
        let _rng = rand::rng();

        // First index documents for deleting
        for (doc_id, content) in &docs {
            index
                .index_document(doc_id.as_str(), content.as_str(), &tokenizer)
                .await;
        }

        println!("Testing delete performance...");
        let mut delete_durations = Vec::with_capacity(operations_per_test);

        for _ in 0..operations_per_test {
            let term = &sample_words[rand::random_range(0..100) % sample_words.len()];
            let (doc_id, _) = &docs[rand::random_range(0..100) % docs.len().saturating_sub(1)];

            let start = Instant::now();
            let _ = index.remove_term(&term, doc_id.as_str()).await.unwrap();
            let duration = start.elapsed();

            delete_durations.push(duration);
        }

        // Save raw performance data to CSV
        if let Err(e) = save_performance_csv("delete_performance", &delete_durations) {
            eprintln!("Failed to save delete performance data to CSV: {}", e);
        }

        let (p10, p50, p90, p99) = calculate_percentiles(&mut delete_durations);
        println!(
            "Delete performance (μs): p10={}, p50={}, p90={}, p99={}",
            p10.as_micros(),
            p50.as_micros(),
            p90.as_micros(),
            p99.as_micros()
        );

        // Clean up
        index_dir.close().unwrap();
    }

    // Test for proper index flushing and persistence
    #[tokio::test]
    async fn test_index_flush() {
        // Create temporary directory using ConfigManager instead of direct path
        use crate::fugu::config::new_config_manager;
        
        let temp_dir = tempdir().unwrap();
        let _config = new_config_manager(Some(temp_dir.path().to_path_buf()));
        
        // Create a unique path for the flush test index
        let flush_dir = temp_dir.path().join("flush_index");
        std::fs::create_dir_all(&flush_dir).unwrap();
        let index_path = flush_dir.to_str().unwrap().to_string();

        // Create the inverted index
        let index = InvertedIndex::new(&index_path).await;

        // Add some test data
        for i in 0..10 {
            let token = Token {
                term: format!("term{}", i),
                doc_id: "test_doc".to_string(),
                position: i,
            };

            index.add_term(token).await.unwrap();
        }

        // Explicitly flush the index to ensure data is persisted
        index.flush().await.unwrap();
        
        // Drop the index to release any locks
        drop(index);
        
        // Wait briefly to ensure resources are released
        // This helps avoid potential race conditions between index instances
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Create a new index instance pointing to the same storage location
        let index2 = InvertedIndex::new(&index_path).await;

        // Verify data persisted after flush
        for i in 0..10 {
            let term = format!("term{}", i);
            let result = index2.search(&term).await.unwrap();

            // The term should be found in the index
            assert!(result.is_some(), "Term {} was not found after flush", term);
            let term_index = result.unwrap();

            // The term should be in the test_doc document
            assert!(
                term_index.doc_ids.contains_key("test_doc"),
                "Term {} was not associated with test_doc after flush",
                term
            );
        }

        // Clean up resources
        drop(index2);
        
        // Clean up the temporary directory
        temp_dir.close().unwrap();
    }

    #[test]
    fn test_add_command_parser() {
        // Test parsing with minimal required arguments
        let args = vec!["add", "--namespace", "test_namespace", "path/to/file.txt"];
        let cmd = AddCommand::parse_from(args);
        
        assert_eq!(cmd.namespace, "test_namespace");
        assert_eq!(cmd.file_path, "path/to/file.txt");
        assert_eq!(cmd.addr, "http://127.0.0.1:50051"); // Default address
        
        // Test parsing with custom server address
        let args = vec!["add", "--namespace", "custom_ns", "--addr", "http://localhost:8080", "path/to/other.txt"];
        let cmd = AddCommand::parse_from(args);
        
        assert_eq!(cmd.namespace, "custom_ns");
        assert_eq!(cmd.file_path, "path/to/other.txt");
        assert_eq!(cmd.addr, "http://localhost:8080");
    }

    #[tokio::test]
    #[cfg(feature = "performance-tests")]
    #[ignore] // Disabled flush tests for visualization and testing
    async fn test_flush_performance() {
        use std::time::{Duration, Instant};

        // Setup test environment
        let temp_dir = tempdir().unwrap();
        let index_path = temp_dir
            .path()
            .join("perf_flush_test")
            .to_str()
            .unwrap()
            .to_string();

        // Create the inverted index
        let index = InvertedIndex::new(&index_path).await;

        // Add a reasonable amount of test data (reduced for faster testing)
        let num_terms = 500;
        let mut flush_durations = Vec::new();
        
        for i in 0..num_terms {
            let token = Token {
                term: format!("performance_term{}", i),
                doc_id: format!("doc{}", i % 100),
                position: i % 1000,
            };

            index.add_term(token).await.unwrap();

            // Flush every 100 terms to simulate periodic flushing
            if i % 100 == 99 {
                let start = Instant::now();
                index.flush().await.unwrap();
                flush_durations.push(start.elapsed());
            }
        }

        // Measure the time to perform a final flush of everything
        let start = Instant::now();
        index.flush().await.unwrap();
        let final_duration = start.elapsed();
        flush_durations.push(final_duration);

        // Save raw performance data to CSV
        if let Err(e) = save_performance_csv("flush_performance", &flush_durations) {
            eprintln!("Failed to save flush performance data to CSV: {}", e);
        }

        println!("Time to flush {} terms: {:?}", num_terms, final_duration);

        // Clean up
        temp_dir.close().unwrap();
    }
}
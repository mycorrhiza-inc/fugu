use futures::{future::poll_fn, pin_mut, stream::BoxStream};
use lazy_static::lazy_static;
use regex::Regex;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::hash_map::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio_stream::{Stream, StreamExt};

#[derive(
    Debug,
    Clone,
    PartialEq,
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
    SerdeSerialize,
    SerdeDeserialize,
)]
pub enum TokenType {
    Word,
    Number,
    AlphaNum,
    Email,
    URL,
    Acronym,
    Host,
    Punctuation,
    PageHeader,
}

#[derive(
    Clone, Copy, Debug, Archive, RkyvDeserialize, RkyvSerialize, SerdeSerialize, SerdeDeserialize,
)]
pub struct TokenPosition {
    pub start: usize,
    pub end: usize,
}

#[derive(Clone, Archive, RkyvDeserialize, RkyvSerialize, SerdeSerialize, SerdeDeserialize)]
pub struct Token {
    pub text: String,
    pub pos: TokenPosition, // Start position in the original text
    pub token_type: TokenType,
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Token({:?}, '{}', position: {}..{})",
            self.token_type, self.text, self.pos.start, self.pos.end
        )
    }
}

pub struct StandardTokenizer<R: AsyncRead + Unpin> {
    reader: BufReader<R>,
    position: usize, // Absolute position in the input stream
    current_line: String,
    current_line_position: usize,
    next_tokens: Vec<Token>,
    token_patterns: Arc<TokenPatterns>,
}

struct TokenPatterns {
    // Regular expressions for different token types
    email_re: Regex,
    url_re: Regex,
    host_re: Regex,
    acronym_re: Regex,
    alphanum_re: Regex,
    number_re: Regex,
    word_re: Regex,
    punct_re: Regex,
    pagehead_re: Regex,
}

// lazy_static! {
//     static ref TOKEN_PATTERNS: Arc<TokenPatterns> = Arc::new(TokenPatterns {
//         email_re: Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap(),
//         url_re: Regex::new(r"^https?://[^\s/$.?#].[^\s]*$").unwrap(),
//         host_re: Regex::new(r"^[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)+$").unwrap(),
//         acronym_re: Regex::new(r"^[A-Z](\.[A-Z])+$").unwrap(),
//         alphanum_re: Regex::new(r"^[a-zA-Z0-9]+$").unwrap(),
//         number_re: Regex::new(r"^[0-9]+(\.[0-9]+)?$").unwrap(),
//         word_re: Regex::new(r"^[a-zA-Z]+$").unwrap(),
//         punct_re: Regex::new(r"^[^\w\s]$").unwrap(),
//         pagehead_re: Regex::new(r"[<!--\s*Page number:\s*\d+\s*-->$").unwrap(),
//     });
// }

impl<R: AsyncRead + Unpin> StandardTokenizer<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            position: 0,
            current_line: String::new(),
            current_line_position: 0,
            next_tokens: Vec::new(),
            token_patterns: Arc::new(TokenPatterns {
                email_re: Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap(),
                url_re: Regex::new(r"^https?://[^\s/$.?#].[^\s]*$").unwrap(),
                host_re: Regex::new(r"^[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)+$").unwrap(),
                acronym_re: Regex::new(r"^[A-Z](\.[A-Z])+$").unwrap(),
                alphanum_re: Regex::new(r"^[a-zA-Z0-9]+$").unwrap(),
                number_re: Regex::new(r"^[0-9]+(\.[0-9]+)?$").unwrap(),
                word_re: Regex::new(r"^[a-zA-Z]+$").unwrap(),
                punct_re: Regex::new(r"^[^\w\s]$").unwrap(),
                pagehead_re: Regex::new(r"<!--\s*Page number:\s*\d+\s*-->$").unwrap(),
            }), // Properly clone the Arc reference
        }
    }

    async fn fill_token_buffer(&mut self) -> Result<(), std::io::Error> {
        if !self.next_tokens.is_empty() {
            return Ok(());
        }

        // If we've processed the current line completely, get a new one
        if self.current_line_position >= self.current_line.len() {
            let bytes_read_before = self.position + self.current_line.len();
            self.current_line.clear();
            let bytes_read = self.reader.read_line(&mut self.current_line).await?;
            if bytes_read == 0 {
                return Ok(()); // EOF
            }
            self.position = bytes_read_before;
            self.current_line_position = 0;
        }

        // Process the current line from our current position
        let line_slice = &self.current_line[self.current_line_position..];

        // Tokenize the remaining part of the line
        let mut current_pos = 0;
        let mut char_indices = line_slice.char_indices().peekable();

        while let Some((i, c)) = char_indices.next() {
            if c.is_whitespace() {
                current_pos = i + c.len_utf8();
                continue;
            }

            // Start of a token
            let token_start = current_pos;
            // Calculate absolute position, accounting for current line and position within file
            let absolute_start = self.position + self.current_line_position + token_start;

            // Consume characters until we hit whitespace or punctuation
            let mut token_text = String::new();
            token_text.push(c);

            // For simplicity, we'll just handle basic word/number tokens
            // In a real implementation, you'd have more complex logic here
            while let Some(&(_, next_c)) = char_indices.peek() {
                if next_c.is_whitespace() || (!next_c.is_alphanumeric() && next_c != '_') {
                    break;
                }
                let (_, consumed_c) = char_indices.next().unwrap();
                token_text.push(consumed_c);
            }

            let token_end = token_start + token_text.len();
            current_pos = token_end;

            // Determine token type
            let token_type = self.determine_token_type(&token_text);

            // Add token to our buffer with absolute position information
            self.next_tokens.push(Token {
                text: token_text,
                pos: TokenPosition {
                    start: absolute_start,
                    end: absolute_start + (token_end - token_start),
                },
                token_type,
            });
        }

        // Update our position in the current line
        self.current_line_position += current_pos;

        Ok(())
    }

    fn determine_token_type(&self, text: &str) -> TokenType {
        // Email has priority
        if self.token_patterns.email_re.is_match(text) {
            return TokenType::Email;
        }

        // URL next
        if self.token_patterns.url_re.is_match(text) {
            return TokenType::URL;
        }

        // Host names
        if self.token_patterns.host_re.is_match(text) {
            return TokenType::Host;
        }

        // Acronyms like U.S.A.
        if self.token_patterns.acronym_re.is_match(text) {
            return TokenType::Acronym;
        }

        // Numbers
        if self.token_patterns.number_re.is_match(text) {
            return TokenType::Number;
        }

        // Words
        if self.token_patterns.word_re.is_match(text) {
            return TokenType::Word;
        }

        // Mix of letters and numbers
        if self.token_patterns.alphanum_re.is_match(text) {
            return TokenType::AlphaNum;
        }
        
        // Pages
        if self.token_patterns.pagehead_re.is_match(text) {
            return TokenType::PageHeader;
        }

        // Default
        TokenType::Punctuation
    }
}

impl<R: AsyncRead + Unpin> Stream for StandardTokenizer<R> {
    type Item = Result<Token, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // If we have tokens ready to return, return one
        if !self.next_tokens.is_empty() {
            return Poll::Ready(Some(Ok(self.next_tokens.remove(0))));
        }

        // We need to fill our token buffer
        let this = &mut *self;

        // Create a future using poll_fn and pin it
        let future = poll_fn(|cx| {
            let future = this.fill_token_buffer();
            pin_mut!(future);
            future.poll(cx)
        });

        // Pin the future and poll it
        let mut pinned_future = Box::pin(future);
        match pinned_future.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => {
                if this.next_tokens.is_empty() {
                    // No more tokens, we're done
                    Poll::Ready(None)
                } else {
                    // Return the first token
                    Poll::Ready(Some(Ok(this.next_tokens.remove(0))))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Helper function to create a tokenizer from any AsyncRead source
pub fn tokenize<R>(reader: R) -> BoxStream<'static, Result<Token, std::io::Error>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let tokenizer = StandardTokenizer::new(reader);
    Box::pin(tokenizer)
}

// Example usage
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example with a string
    let text = "Hello world! This is a sample text with email@example.com and https://www.example.com URLs.";
    let cursor = std::io::Cursor::new(text.as_bytes());

    let mut token_stream = tokenize(cursor);

    println!("Token Stream Results:");
    println!("---------------------");
    println!("Type       | Text                   | Position");
    println!("---------------------");

    while let Some(token_result) = token_stream.next().await {
        match token_result {
            Ok(token) => {
                println!(
                    "{:<10} | {:<22} | {}-{}",
                    format!("{:?}", token.token_type),
                    token.text,
                    token.pos.start,
                    token.pos.end
                );
            }
            Err(e) => eprintln!("Error: {}", e),
        }
    }
    Ok(())
}

// Special tokenizer for strings that doesn't require thread safety

pub async fn tokenize_into_index(
    text: &str,
    field_index: &mut HashMap<String, Vec<TokenPosition>>,
) -> Result<(), std::io::Error> {
    // Create owned copy of the text to avoid lifetime issues
    let text_owned = text.to_string();
    let cursor = std::io::Cursor::new(text_owned);

    // Create a tokenizer directly instead of using the tokenize function
    // which returns a 'static stream
    let mut tokenizer = StandardTokenizer::new(cursor);

    while let Some(token_result) = StreamExt::next(&mut tokenizer).await {
        match token_result {
            Ok(token) => {
                // Skip punctuation tokens for indexing
                if matches!(
                    token.token_type,
                    TokenType::Punctuation | TokenType::PageHeader
                ) {
                    continue;
                }

                // Normalize to lowercase for indexing
                let normalized_word = token.text.to_lowercase();

                match field_index.entry(normalized_word) {
                    Occupied(mut o) => {
                        o.get_mut().push(token.pos);
                    }
                    Vacant(v) => {
                        v.insert(vec![token.pos]);
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

// Update tokenize_query to use the StandardTokenizer
pub async fn tokenize_query(text: &str, tokens: &mut Vec<Token>) -> Result<(), std::io::Error> {
    // Create owned copy of the text to avoid lifetime issues
    let text_owned = text.to_string();
    let cursor = std::io::Cursor::new(text_owned);

    // Create a tokenizer directly instead of using the tokenize function
    let mut tokenizer = StandardTokenizer::new(cursor);

    while let Some(token_result) = StreamExt::next(&mut tokenizer).await {
        match token_result {
            Ok(token) => {
                tokens.push(token);
            }
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

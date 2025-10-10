//! Full-Text Search Implementation
//!
//! Provides comprehensive full-text search capabilities including:
//! - Text indexing with TF-IDF scoring
//! - Tokenization with multiple language support
//! - Boolean search queries (AND, OR, NOT)
//! - Phrase search and proximity queries
//! - Fuzzy matching with edit distance
//! - Stemming and stop word filtering
//! - Search result ranking and highlighting

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, trace};

use crate::errors::{DriftError, Result};

/// Full-text search index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchConfig {
    /// Minimum word length to index
    pub min_word_length: usize,
    /// Maximum word length to index
    pub max_word_length: usize,
    /// Whether to enable stemming
    pub enable_stemming: bool,
    /// Whether to filter stop words
    pub filter_stop_words: bool,
    /// Language for tokenization and stemming
    pub language: String,
    /// Case sensitive search
    pub case_sensitive: bool,
    /// Maximum number of search results
    pub max_results: usize,
    /// Enable fuzzy matching
    pub enable_fuzzy: bool,
    /// Maximum edit distance for fuzzy matching
    pub max_edit_distance: usize,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            min_word_length: 2,
            max_word_length: 50,
            enable_stemming: true,
            filter_stop_words: true,
            language: "english".to_string(),
            case_sensitive: false,
            max_results: 100,
            enable_fuzzy: false,
            max_edit_distance: 2,
        }
    }
}

/// A full-text search index
#[derive(Debug, Clone)]
pub struct SearchIndex {
    /// Index name
    pub name: String,
    /// Table and column being indexed
    pub table: String,
    pub column: String,
    /// Configuration
    pub config: SearchConfig,
    /// Forward index: document_id -> terms
    pub forward_index: HashMap<String, Vec<String>>,
    /// Inverted index: term -> document frequencies
    pub inverted_index: HashMap<String, TermInfo>,
    /// Document metadata
    pub documents: HashMap<String, DocumentInfo>,
    /// Total number of documents
    pub total_documents: usize,
    /// Index creation/update time
    pub updated_at: SystemTime,
}

/// Information about a term in the index
#[derive(Debug, Clone)]
pub struct TermInfo {
    /// Documents containing this term
    pub documents: HashMap<String, TermFrequency>,
    /// Total frequency across all documents
    pub total_frequency: usize,
    /// Number of documents containing this term
    pub document_frequency: usize,
}

/// Term frequency information for a document
#[derive(Debug, Clone)]
pub struct TermFrequency {
    /// Number of occurrences in the document
    pub frequency: usize,
    /// Positions where the term appears
    pub positions: Vec<usize>,
    /// TF-IDF score for this term in this document
    pub tf_idf: f64,
}

/// Document information
#[derive(Debug, Clone)]
pub struct DocumentInfo {
    /// Primary key of the document
    pub primary_key: String,
    /// Full text content
    pub content: String,
    /// Number of terms in the document
    pub term_count: usize,
    /// When this document was indexed
    pub indexed_at: SystemTime,
}

/// Search query types
#[derive(Debug, Clone)]
pub enum SearchQuery {
    /// Simple text search
    Text(String),
    /// Boolean query with operators
    Boolean(BooleanQuery),
    /// Phrase search (exact sequence)
    Phrase(String),
    /// Proximity search (terms within N words)
    Proximity { terms: Vec<String>, distance: usize },
    /// Fuzzy search with edit distance
    Fuzzy { term: String, max_distance: usize },
}

/// Boolean search query
#[derive(Debug, Clone)]
pub enum BooleanQuery {
    And(Box<BooleanQuery>, Box<BooleanQuery>),
    Or(Box<BooleanQuery>, Box<BooleanQuery>),
    Not(Box<BooleanQuery>),
    Term(String),
    Phrase(String),
}

/// Search result
#[derive(Debug, Clone, Serialize)]
pub struct SearchResult {
    /// Document primary key
    pub document_id: String,
    /// Relevance score
    pub score: f64,
    /// Matched terms
    pub matched_terms: Vec<String>,
    /// Highlighted snippets
    pub snippets: Vec<String>,
    /// Document content (if requested)
    pub content: Option<String>,
}

/// Search results with metadata
#[derive(Debug, Clone, Serialize)]
pub struct SearchResults {
    /// Matching documents
    pub results: Vec<SearchResult>,
    /// Total number of matches (before pagination)
    pub total_matches: usize,
    /// Query execution time in milliseconds
    pub execution_time_ms: u64,
    /// Search statistics
    pub stats: SearchStats,
}

/// Search execution statistics
#[derive(Debug, Clone, Serialize)]
pub struct SearchStats {
    /// Number of terms processed
    pub terms_processed: usize,
    /// Number of documents scored
    pub documents_scored: usize,
    /// Whether fuzzy matching was used
    pub used_fuzzy: bool,
    /// Index utilization percentage
    pub index_utilization: f64,
}

/// Full-text search manager
pub struct SearchManager {
    /// All search indexes by name
    indexes: Arc<RwLock<HashMap<String, SearchIndex>>>,
    /// Indexes by table for quick lookup
    table_indexes: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Search statistics
    stats: Arc<RwLock<GlobalSearchStats>>,
    /// Tokenizer
    tokenizer: Arc<Tokenizer>,
}

/// Global search statistics
#[derive(Debug, Default, Clone, Serialize)]
pub struct GlobalSearchStats {
    pub total_indexes: usize,
    pub total_documents: usize,
    pub total_searches: u64,
    pub avg_search_time_ms: f64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

/// Text tokenizer
pub struct Tokenizer {
    stop_words: HashSet<String>,
    stemmer: Option<Stemmer>,
}

/// Simple stemmer implementation
pub struct Stemmer;

impl Stemmer {
    /// Apply stemming to a word
    pub fn stem(&self, word: &str) -> String {
        // Simple English stemming rules
        let word = word.to_lowercase();

        // Remove common suffixes
        if word.ends_with("ing") && word.len() > 6 {
            return word[..word.len() - 3].to_string();
        }
        if word.ends_with("ed") && word.len() > 5 {
            return word[..word.len() - 2].to_string();
        }
        if word.ends_with("er") && word.len() > 5 {
            return word[..word.len() - 2].to_string();
        }
        if word.ends_with("est") && word.len() > 6 {
            return word[..word.len() - 3].to_string();
        }
        if word.ends_with("ly") && word.len() > 5 {
            return word[..word.len() - 2].to_string();
        }
        if word.ends_with("tion") && word.len() > 7 {
            return word[..word.len() - 4].to_string();
        }
        if word.ends_with("sion") && word.len() > 7 {
            return word[..word.len() - 4].to_string();
        }

        word
    }
}

impl Tokenizer {
    /// Create a new tokenizer
    pub fn new(language: &str) -> Self {
        let stop_words = Self::load_stop_words(language);
        let stemmer = if language == "english" {
            Some(Stemmer)
        } else {
            None
        };

        Self {
            stop_words,
            stemmer,
        }
    }

    /// Tokenize text into terms
    pub fn tokenize(&self, text: &str, config: &SearchConfig) -> Vec<String> {
        let text = if config.case_sensitive {
            text.to_string()
        } else {
            text.to_lowercase()
        };

        // Split into words using common delimiters
        let words: Vec<&str> = text
            .split(|c: char| c.is_whitespace() || c.is_ascii_punctuation())
            .filter(|w| !w.is_empty())
            .collect();

        let mut terms = Vec::new();

        for word in words {
            // Check length constraints
            if word.len() < config.min_word_length || word.len() > config.max_word_length {
                continue;
            }

            // Filter stop words
            if config.filter_stop_words && self.stop_words.contains(word) {
                continue;
            }

            // Apply stemming
            let term = if config.enable_stemming {
                if let Some(stemmer) = &self.stemmer {
                    stemmer.stem(word)
                } else {
                    word.to_string()
                }
            } else {
                word.to_string()
            };

            terms.push(term);
        }

        terms
    }

    /// Load stop words for a language
    fn load_stop_words(language: &str) -> HashSet<String> {
        let stop_words = match language {
            "english" => vec![
                "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "he", "in",
                "is", "it", "its", "of", "on", "that", "the", "to", "was", "were", "will", "with",
                "but", "not", "or", "this", "have", "had", "what", "when", "where", "who", "which",
                "why", "how",
            ],
            _ => vec![], // No stop words for unknown languages
        };

        stop_words.into_iter().map(|s| s.to_string()).collect()
    }
}

impl SearchManager {
    /// Create a new search manager
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
            table_indexes: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(GlobalSearchStats::default())),
            tokenizer: Arc::new(Tokenizer::new("english")),
        }
    }

    /// Create a full-text search index
    pub fn create_index(
        &self,
        name: String,
        table: String,
        column: String,
        config: SearchConfig,
    ) -> Result<()> {
        debug!(
            "Creating full-text index '{}' on {}.{}",
            name, table, column
        );

        let index = SearchIndex {
            name: name.clone(),
            table: table.clone(),
            column,
            config,
            forward_index: HashMap::new(),
            inverted_index: HashMap::new(),
            documents: HashMap::new(),
            total_documents: 0,
            updated_at: SystemTime::now(),
        };

        // Add to indexes
        {
            let mut indexes = self.indexes.write();
            if indexes.contains_key(&name) {
                return Err(DriftError::InvalidQuery(format!(
                    "Search index '{}' already exists",
                    name
                )));
            }
            indexes.insert(name.clone(), index);
        }

        // Add to table indexes
        {
            let mut table_indexes = self.table_indexes.write();
            table_indexes
                .entry(table)
                .or_insert_with(Vec::new)
                .push(name.clone());
        }

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_indexes += 1;
        }

        info!("Full-text index '{}' created successfully", name);
        Ok(())
    }

    /// Drop a search index
    pub fn drop_index(&self, name: &str) -> Result<()> {
        debug!("Dropping search index '{}'", name);

        let table_name = {
            let mut indexes = self.indexes.write();
            let index = indexes.remove(name).ok_or_else(|| {
                DriftError::InvalidQuery(format!("Search index '{}' does not exist", name))
            })?;
            index.table
        };

        // Remove from table indexes
        {
            let mut table_indexes = self.table_indexes.write();
            if let Some(table_idx) = table_indexes.get_mut(&table_name) {
                table_idx.retain(|idx| idx != name);
                if table_idx.is_empty() {
                    table_indexes.remove(&table_name);
                }
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_indexes = stats.total_indexes.saturating_sub(1);
        }

        info!("Search index '{}' dropped", name);
        Ok(())
    }

    /// Add a document to the search index
    pub fn index_document(
        &self,
        index_name: &str,
        document_id: String,
        content: String,
    ) -> Result<()> {
        let mut indexes = self.indexes.write();
        let index = indexes.get_mut(index_name).ok_or_else(|| {
            DriftError::InvalidQuery(format!("Search index '{}' does not exist", index_name))
        })?;

        trace!(
            "Indexing document '{}' in index '{}'",
            document_id,
            index_name
        );

        // Tokenize the content
        let terms = self.tokenizer.tokenize(&content, &index.config);

        // Remove existing document if it exists
        if index.documents.contains_key(&document_id) {
            self.remove_document_from_index(index, &document_id);
        }

        // Count term frequencies and positions
        let mut term_counts: HashMap<String, Vec<usize>> = HashMap::new();
        for (pos, term) in terms.iter().enumerate() {
            term_counts
                .entry(term.clone())
                .or_insert_with(Vec::new)
                .push(pos);
        }

        // Add to forward index
        index
            .forward_index
            .insert(document_id.clone(), terms.clone());

        // Add document info
        let doc_info = DocumentInfo {
            primary_key: document_id.clone(),
            content: content.clone(),
            term_count: terms.len(),
            indexed_at: SystemTime::now(),
        };
        index.documents.insert(document_id.clone(), doc_info);

        // Update inverted index
        for (term, positions) in term_counts {
            let term_freq = TermFrequency {
                frequency: positions.len(),
                positions,
                tf_idf: 0.0, // Will be calculated later
            };

            index
                .inverted_index
                .entry(term.clone())
                .or_insert_with(|| TermInfo {
                    documents: HashMap::new(),
                    total_frequency: 0,
                    document_frequency: 0,
                })
                .documents
                .insert(document_id.clone(), term_freq);
        }

        // Update document count
        index.total_documents += 1;
        index.updated_at = SystemTime::now();

        // Recalculate TF-IDF scores
        self.calculate_tf_idf(index);

        Ok(())
    }

    /// Remove a document from the search index
    fn remove_document_from_index(&self, index: &mut SearchIndex, document_id: &str) {
        if let Some(terms) = index.forward_index.remove(document_id) {
            // Remove from inverted index
            for term in terms {
                if let Some(term_info) = index.inverted_index.get_mut(&term) {
                    term_info.documents.remove(document_id);
                    term_info.document_frequency = term_info.documents.len();

                    if term_info.documents.is_empty() {
                        index.inverted_index.remove(&term);
                    }
                }
            }

            // Remove document info
            index.documents.remove(document_id);
            index.total_documents = index.total_documents.saturating_sub(1);
        }
    }

    /// Calculate TF-IDF scores for all terms
    fn calculate_tf_idf(&self, index: &mut SearchIndex) {
        let total_docs = index.total_documents as f64;

        for term_info in index.inverted_index.values_mut() {
            let idf = (total_docs / term_info.document_frequency as f64).ln();

            for (doc_id, term_freq) in &mut term_info.documents {
                if let Some(doc_info) = index.documents.get(doc_id) {
                    let tf = term_freq.frequency as f64 / doc_info.term_count as f64;
                    term_freq.tf_idf = tf * idf;
                }
            }
        }
    }

    /// Search the index
    pub fn search(
        &self,
        index_name: &str,
        query: SearchQuery,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<SearchResults> {
        let start_time = std::time::Instant::now();

        let indexes = self.indexes.read();
        let index = indexes.get(index_name).ok_or_else(|| {
            DriftError::InvalidQuery(format!("Search index '{}' does not exist", index_name))
        })?;

        let limit = limit
            .unwrap_or(index.config.max_results)
            .min(index.config.max_results);
        let offset = offset.unwrap_or(0);

        let mut document_scores: HashMap<String, f64> = HashMap::new();
        let mut matched_terms: HashMap<String, Vec<String>> = HashMap::new();
        let terms_processed;

        match query {
            SearchQuery::Text(text) => {
                let terms = self.tokenizer.tokenize(&text, &index.config);
                terms_processed = terms.len();

                for term in &terms {
                    if let Some(term_info) = index.inverted_index.get(term) {
                        for (doc_id, term_freq) in &term_info.documents {
                            *document_scores.entry(doc_id.clone()).or_insert(0.0) +=
                                term_freq.tf_idf;
                            matched_terms
                                .entry(doc_id.clone())
                                .or_insert_with(Vec::new)
                                .push(term.clone());
                        }
                    }
                }
            }
            SearchQuery::Phrase(phrase) => {
                let terms = self.tokenizer.tokenize(&phrase, &index.config);
                terms_processed = terms.len();

                if !terms.is_empty() {
                    let phrase_matches = self.find_phrase_matches(index, &terms);
                    for (doc_id, score) in phrase_matches {
                        document_scores.insert(doc_id.clone(), score);
                        matched_terms.insert(doc_id, terms.clone());
                    }
                }
            }
            SearchQuery::Boolean(bool_query) => {
                let matches = self.evaluate_boolean_query(index, &bool_query);
                for doc_id in matches {
                    document_scores.insert(doc_id.clone(), 1.0);
                    matched_terms.insert(doc_id, vec!["boolean".to_string()]);
                }
                terms_processed = 1; // Simplified for boolean queries
            }
            _ => {
                // TODO: Implement other query types
                return Err(DriftError::InvalidQuery(
                    "Query type not yet implemented".to_string(),
                ));
            }
        }

        // Sort by score (descending)
        let mut scored_docs: Vec<(String, f64)> = document_scores.into_iter().collect();
        scored_docs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Apply pagination
        let total_matches = scored_docs.len();
        let paginated_docs: Vec<_> = scored_docs.into_iter().skip(offset).take(limit).collect();

        // Build results
        let mut results = Vec::new();
        for (doc_id, score) in paginated_docs {
            if let Some(doc_info) = index.documents.get(&doc_id) {
                let snippets = self.generate_snippets(
                    &doc_info.content,
                    matched_terms.get(&doc_id).unwrap_or(&vec![]),
                    150,
                );

                results.push(SearchResult {
                    document_id: doc_id.clone(),
                    score,
                    matched_terms: matched_terms.get(&doc_id).cloned().unwrap_or_default(),
                    snippets,
                    content: None,
                });
            }
        }

        let execution_time = start_time.elapsed().as_millis() as u64;

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_searches += 1;
            let total_time = stats.avg_search_time_ms * (stats.total_searches - 1) as f64;
            stats.avg_search_time_ms =
                (total_time + execution_time as f64) / stats.total_searches as f64;
        }

        Ok(SearchResults {
            results,
            total_matches,
            execution_time_ms: execution_time,
            stats: SearchStats {
                terms_processed,
                documents_scored: total_matches,
                used_fuzzy: false,
                index_utilization: (total_matches as f64 / index.total_documents as f64) * 100.0,
            },
        })
    }

    /// Find phrase matches in the index
    fn find_phrase_matches(&self, index: &SearchIndex, terms: &[String]) -> HashMap<String, f64> {
        let mut matches = HashMap::new();

        if terms.is_empty() {
            return matches;
        }

        // Get documents containing the first term
        if let Some(first_term_info) = index.inverted_index.get(&terms[0]) {
            for (doc_id, _) in &first_term_info.documents {
                if self.document_contains_phrase(index, doc_id, terms) {
                    // Calculate phrase score (sum of individual term TF-IDF scores)
                    let mut score = 0.0;
                    for term in terms {
                        if let Some(term_info) = index.inverted_index.get(term) {
                            if let Some(term_freq) = term_info.documents.get(doc_id) {
                                score += term_freq.tf_idf;
                            }
                        }
                    }
                    matches.insert(doc_id.clone(), score);
                }
            }
        }

        matches
    }

    /// Check if a document contains a phrase
    fn document_contains_phrase(
        &self,
        index: &SearchIndex,
        doc_id: &str,
        terms: &[String],
    ) -> bool {
        if let Some(doc_terms) = index.forward_index.get(doc_id) {
            // Look for consecutive occurrences of the phrase terms
            for i in 0..doc_terms.len().saturating_sub(terms.len() - 1) {
                if doc_terms[i..i + terms.len()] == *terms {
                    return true;
                }
            }
        }
        false
    }

    /// Evaluate a boolean query
    fn evaluate_boolean_query(&self, index: &SearchIndex, query: &BooleanQuery) -> HashSet<String> {
        match query {
            BooleanQuery::Term(term) => {
                if let Some(term_info) = index.inverted_index.get(term) {
                    term_info.documents.keys().cloned().collect()
                } else {
                    HashSet::new()
                }
            }
            BooleanQuery::Phrase(phrase) => {
                let terms = self.tokenizer.tokenize(phrase, &index.config);
                self.find_phrase_matches(index, &terms)
                    .keys()
                    .cloned()
                    .collect()
            }
            BooleanQuery::And(left, right) => {
                let left_docs = self.evaluate_boolean_query(index, left);
                let right_docs = self.evaluate_boolean_query(index, right);
                left_docs.intersection(&right_docs).cloned().collect()
            }
            BooleanQuery::Or(left, right) => {
                let left_docs = self.evaluate_boolean_query(index, left);
                let right_docs = self.evaluate_boolean_query(index, right);
                left_docs.union(&right_docs).cloned().collect()
            }
            BooleanQuery::Not(inner) => {
                let inner_docs = self.evaluate_boolean_query(index, inner);
                let all_docs: HashSet<String> = index.documents.keys().cloned().collect();
                all_docs.difference(&inner_docs).cloned().collect()
            }
        }
    }

    /// Generate text snippets with highlighted terms
    fn generate_snippets(&self, content: &str, terms: &[String], max_length: usize) -> Vec<String> {
        let mut snippets = Vec::new();
        let words: Vec<&str> = content.split_whitespace().collect();

        if words.is_empty() {
            return snippets;
        }

        // Find positions of matching terms
        let mut match_positions = Vec::new();
        for (pos, word) in words.iter().enumerate() {
            let word_lower = word.to_lowercase();
            for term in terms {
                if word_lower.contains(&term.to_lowercase()) {
                    match_positions.push(pos);
                    break;
                }
            }
        }

        if match_positions.is_empty() {
            // No matches found, return beginning of content
            let snippet: String = words.iter().take(20).cloned().collect::<Vec<_>>().join(" ");
            if snippet.len() <= max_length {
                snippets.push(snippet);
            } else {
                snippets.push(format!("{}...", &snippet[..max_length.saturating_sub(3)]));
            }
            return snippets;
        }

        // Generate snippets around match positions
        for &match_pos in &match_positions {
            let start = match_pos.saturating_sub(10);
            let end = (match_pos + 10).min(words.len());

            let snippet_words = &words[start..end];
            let snippet = snippet_words.join(" ");

            let highlighted = self.highlight_terms(&snippet, terms);

            if highlighted.len() <= max_length {
                snippets.push(highlighted);
            } else {
                snippets.push(format!(
                    "{}...",
                    &highlighted[..max_length.saturating_sub(3)]
                ));
            }
        }

        // Remove duplicates and limit number of snippets
        snippets.sort();
        snippets.dedup();
        snippets.truncate(3);

        snippets
    }

    /// Highlight search terms in text
    fn highlight_terms(&self, text: &str, terms: &[String]) -> String {
        let mut highlighted = text.to_string();

        for term in terms {
            // Simple highlighting with **term**
            let pattern = regex::Regex::new(&format!(r"(?i)\b{}\b", regex::escape(term)))
                .unwrap_or_else(|_| regex::Regex::new(term).unwrap());
            highlighted = pattern.replace_all(&highlighted, "**$0**").to_string();
        }

        highlighted
    }

    /// Get search statistics
    pub fn statistics(&self) -> GlobalSearchStats {
        self.stats.read().clone()
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.read().keys().cloned().collect()
    }

    /// Get index information
    pub fn get_index_info(&self, name: &str) -> Option<(String, String, usize, SystemTime)> {
        self.indexes.read().get(name).map(|idx| {
            (
                idx.table.clone(),
                idx.column.clone(),
                idx.total_documents,
                idx.updated_at,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenizer() {
        let tokenizer = Tokenizer::new("english");
        let config = SearchConfig::default();

        let tokens = tokenizer.tokenize("The quick brown fox jumps over the lazy dog", &config);

        // Should filter stop words like "the", "over"
        assert!(!tokens.contains(&"the".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
        assert!(tokens.contains(&"brown".to_string()));
    }

    #[test]
    fn test_stemmer() {
        let stemmer = Stemmer;

        assert_eq!(stemmer.stem("running"), "run");
        assert_eq!(stemmer.stem("jumped"), "jump");
        assert_eq!(stemmer.stem("faster"), "fast");
        assert_eq!(stemmer.stem("information"), "informat");
    }

    #[test]
    fn test_search_index_creation() {
        let manager = SearchManager::new();

        manager
            .create_index(
                "test_index".to_string(),
                "documents".to_string(),
                "content".to_string(),
                SearchConfig::default(),
            )
            .unwrap();

        let indexes = manager.list_indexes();
        assert!(indexes.contains(&"test_index".to_string()));
    }

    #[test]
    fn test_document_indexing() {
        let manager = SearchManager::new();

        manager
            .create_index(
                "test_index".to_string(),
                "documents".to_string(),
                "content".to_string(),
                SearchConfig::default(),
            )
            .unwrap();

        manager
            .index_document(
                "test_index",
                "doc1".to_string(),
                "The quick brown fox jumps over the lazy dog".to_string(),
            )
            .unwrap();

        let (_, _, doc_count, _) = manager.get_index_info("test_index").unwrap();
        assert_eq!(doc_count, 1);
    }

    #[test]
    fn test_text_search() {
        let manager = SearchManager::new();

        manager
            .create_index(
                "test_index".to_string(),
                "documents".to_string(),
                "content".to_string(),
                SearchConfig::default(),
            )
            .unwrap();

        manager
            .index_document(
                "test_index",
                "doc1".to_string(),
                "The quick brown fox".to_string(),
            )
            .unwrap();

        manager
            .index_document(
                "test_index",
                "doc2".to_string(),
                "The lazy dog sleeps".to_string(),
            )
            .unwrap();

        let results = manager
            .search(
                "test_index",
                SearchQuery::Text("quick".to_string()),
                None,
                None,
            )
            .unwrap();

        assert_eq!(results.results.len(), 1);
        assert_eq!(results.results[0].document_id, "doc1");
    }
}

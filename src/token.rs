use std::collections::HashSet;

static BERT_BASE_UNCASED_BYTES: &[u8] = include_bytes!("../tokenizer/bert_base_uncased.json");

const TOKEN_PATTERN: &str = r"(?u)\b\w\w+\b";

lazy_static::lazy_static! {
    static ref TOKEN_PATTERN_RE: regex::Regex = regex::Regex::new(TOKEN_PATTERN).unwrap();
    static ref STOP_WORDS: HashSet<String> = {
        let words = stop_words::get(stop_words::LANGUAGE::English);
        words.into_iter().collect()
    };

    static ref TOKENIZER: BertWithStemmerAndSplit = Default::default();
}

pub fn tokenize(text: &str) -> Vec<u32> {
    TOKENIZER.encode(text)
}

pub fn vocab_len() -> u32 {
    TOKENIZER.vocab_len()
}

trait Tokenizer {
    fn encode(&self, text: &str) -> Vec<u32>;
    fn vocab_len(&self) -> u32;
}

struct BertWithStemmerAndSplit(tokenizers::Tokenizer);

impl Default for BertWithStemmerAndSplit {
    fn default() -> Self {
        Self(tokenizers::Tokenizer::from_bytes(BERT_BASE_UNCASED_BYTES).unwrap())
    }
}

impl Tokenizer for BertWithStemmerAndSplit {
    fn encode(&self, text: &str) -> Vec<u32> {
        let mut results = Vec::new();
        let lower_text = text.to_lowercase();
        let split = TOKEN_PATTERN_RE.find_iter(&lower_text);
        for token in split {
            if STOP_WORDS.contains(token.as_str()) {
                continue;
            }
            let stemmed_token =
                tantivy_stemmers::algorithms::english_porter_2(token.as_str()).to_string();
            let encoding = self.0.encode_fast(stemmed_token, false).unwrap();
            results.extend_from_slice(encoding.get_ids());
        }
        results
    }

    fn vocab_len(&self) -> u32 {
        self.0.get_vocab_size(false) as u32
    }
}

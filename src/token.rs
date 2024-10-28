static BERT_BASE_UNCASED_BYTES: &[u8] = include_bytes!("../tokenizer/bert_base_uncased.json");

lazy_static::lazy_static! {
    static ref TOKENIZER: BertWithStemmerAndStop = Default::default();
}

pub fn tokenize(text: &str) -> impl AsRef<[u32]> + '_ {
    TOKENIZER.encode(text)
}

pub fn vocab_len() -> u32 {
    TOKENIZER.vocab_len()
}

trait Tokenizer {
    fn encode(&self, text: &str) -> impl AsRef<[u32]> + '_;
    fn vocab_len(&self) -> u32;
}

struct O200kBase(tiktoken_rs::CoreBPE);

impl Default for O200kBase {
    fn default() -> Self {
        Self(tiktoken_rs::o200k_base().unwrap())
    }
}

impl Tokenizer for O200kBase {
    fn encode(&self, text: &str) -> impl AsRef<[u32]> + '_ {
        self.0.encode_ordinary(text)
    }

    fn vocab_len(&self) -> u32 {
        199997
    }
}

struct BertWithStemmer(tokenizers::Tokenizer);

impl Default for BertWithStemmer {
    fn default() -> Self {
        Self(tokenizers::Tokenizer::from_bytes(BERT_BASE_UNCASED_BYTES).unwrap())
    }
}

impl Tokenizer for BertWithStemmer {
    fn encode(&self, text: &str) -> impl AsRef<[u32]> + '_ {
        let text = tantivy_stemmers::algorithms::english_porter_2(text);
        let encoding = self.0.encode_fast(text.as_ref(), false).unwrap();

        struct Tmp {
            encoding: tokenizers::Encoding,
        }
        impl AsRef<[u32]> for Tmp {
            fn as_ref(&self) -> &[u32] {
                self.encoding.get_ids()
            }
        }

        Tmp { encoding }
    }

    fn vocab_len(&self) -> u32 {
        self.0.get_vocab_size(false) as u32
    }
}

struct BertWithStemmerAndStop(tokenizers::Tokenizer);

impl Default for BertWithStemmerAndStop {
    fn default() -> Self {
        Self(tokenizers::Tokenizer::from_bytes(BERT_BASE_UNCASED_BYTES).unwrap())
    }
}

impl Tokenizer for BertWithStemmerAndStop {
    fn encode(&self, text: &str) -> impl AsRef<[u32]> + '_ {
        let words = stop_words::get(stop_words::LANGUAGE::English);
        let lowercase_doc = text.to_lowercase();
        let regex_for_punctuation = human_regex::one_or_more(human_regex::punctuation());
        let text_without_punctuation = regex_for_punctuation
            .to_regex()
            .replace_all(&lowercase_doc, "");

        let regex_for_stop_words = human_regex::word_boundary()
            + human_regex::exactly(1, human_regex::or(&words))
            + human_regex::word_boundary()
            + human_regex::one_or_more(human_regex::whitespace());
        let clean_text = regex_for_stop_words
            .to_regex()
            .replace_all(&text_without_punctuation, "");

        let stemmer_text = tantivy_stemmers::algorithms::english_porter_2(clean_text.as_ref());
        let encoding = self.0.encode_fast(stemmer_text.as_ref(), false).unwrap();

        struct Tmp {
            encoding: tokenizers::Encoding,
        }
        impl AsRef<[u32]> for Tmp {
            fn as_ref(&self) -> &[u32] {
                self.encoding.get_ids()
            }
        }

        Tmp { encoding }
    }

    fn vocab_len(&self) -> u32 {
        self.0.get_vocab_size(false) as u32
    }
}

static BERT_BASE_UNCASED_BYTES: &[u8] = include_bytes!("../tokenizer/bert_base_uncased.json");

lazy_static::lazy_static! {
    pub static ref TOKENIZER: tokenizers::Tokenizer =
        tokenizers::Tokenizer::from_bytes(BERT_BASE_UNCASED_BYTES).unwrap();

    pub static ref VOCAB_LEN: u32 = TOKENIZER.get_vocab_size(false) as u32;
}

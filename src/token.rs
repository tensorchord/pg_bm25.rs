use std::collections::{HashMap, HashSet};

use pgrx::{
    extension_sql_file, pg_sys::panic::ErrorReportable, pg_trigger, prelude::PgHeapTuple,
    spi::SpiClient, IntoDatum, WhoAllocated,
};
use serde::{Deserialize, Serialize};
use tocken::tokenizer::Tokenizer as Tockenizer;
use unicode_segmentation::UnicodeSegmentation;
use validator::{Validate, ValidationError};

use crate::datatype::Bm25VectorOutput;

static BERT_BASE_UNCASED_BYTES: &[u8] = include_bytes!("../tokenizer/bert_base_uncased.json");
static TOCKEN: &[u8] = include_bytes!("../tokenizer/wiki_tocken.json");

const TOKEN_PATTERN: &str = r"(?u)\b\w\w+\b";

lazy_static::lazy_static! {
    static ref TOKEN_PATTERN_RE: regex::Regex = regex::Regex::new(TOKEN_PATTERN).unwrap();
    pub static ref STOP_WORDS_LUCENE: HashSet<String> = {
        [
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
            "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
            "these", "they", "this", "to", "was", "will", "with",
        ].iter().map(|s| s.to_string()).collect()
    };
    pub static ref STOP_WORDS_NLTK: HashSet<String> = {
        let words = stop_words::get(stop_words::LANGUAGE::English);
        words.into_iter().collect()
    };

    static ref BERT_TOKENIZER: BertWithStemmerAndSplit = BertWithStemmerAndSplit::new();
    static ref TOCKENIZER: Tocken = Tocken::new();
}

struct BertWithStemmerAndSplit(tokenizers::Tokenizer);

impl BertWithStemmerAndSplit {
    fn new() -> Self {
        Self(tokenizers::Tokenizer::from_bytes(BERT_BASE_UNCASED_BYTES).unwrap())
    }

    fn encode(&self, text: &str) -> Vec<u32> {
        let mut results = Vec::new();
        let lower_text = text.to_lowercase();
        let split = TOKEN_PATTERN_RE.find_iter(&lower_text);
        for token in split {
            if STOP_WORDS_NLTK.contains(token.as_str()) {
                continue;
            }
            let stemmed_token =
                tantivy_stemmers::algorithms::english_porter_2(token.as_str()).to_string();
            let encoding = self.0.encode_fast(stemmed_token, false).unwrap();
            results.extend_from_slice(encoding.get_ids());
        }
        results
    }
}

struct Tocken(Tockenizer);

impl Tocken {
    fn new() -> Self {
        Self(tocken::tokenizer::Tokenizer::loads(
            std::str::from_utf8(TOCKEN).unwrap(),
        ))
    }

    fn encode(&self, text: &str) -> Vec<u32> {
        self.0.tokenize(text)
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
pub fn unicode_tokenizer_split(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    for word in text.unicode_words() {
        // trim `'s` for English
        let mut lowercase = word.to_lowercase();
        if lowercase.len() >= 2 && lowercase.ends_with("s") {
            let chars = lowercase.chars().collect::<Vec<char>>();
            let c = chars[chars.len() - 2];
            if c == '\'' || c == '\u{2019}' || c == '\u{FF07}' {
                lowercase = chars[..chars.len() - 2].iter().collect::<String>();
            }
        }
        let token = tantivy_stemmers::algorithms::english_porter(&lowercase).to_string();
        if token.is_empty() {
            continue;
        }
        if !STOP_WORDS_LUCENE.contains(&lowercase) {
            tokens.push(token.clone());
        }
        if !STOP_WORDS_NLTK.contains(&lowercase) {
            tokens.push(token);
        }
    }
    tokens
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[repr(i32)]
enum TokenizerKind {
    Bert,
    Tocken,
    Unicode,
}

#[derive(Clone, Serialize, Deserialize, Validate)]
#[validate(schema(function = "TokenizerConfig::validate_unicode"))]
#[serde(deny_unknown_fields)]
struct TokenizerConfig {
    tokenizer: TokenizerKind,
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    column: Option<String>,
}

impl TokenizerConfig {
    fn validate_unicode(&self) -> Result<(), ValidationError> {
        if !matches!(self.tokenizer, TokenizerKind::Unicode) {
            return Ok(());
        }
        if self.table.is_none() {
            return Err(ValidationError::new(
                "table is required for unicode tokenizer",
            ));
        }
        if self.column.is_none() {
            return Err(ValidationError::new(
                "column is required for unicode tokenizer",
            ));
        }
        Ok(())
    }
}

extension_sql_file!(
    "sql/tokenizer.sql",
    name = "tokenizer_table",
    requires = [unicode_tokenizer_split]
);

#[pgrx::pg_extern(requires = ["tokenizer_table"])]
pub fn create_tokenizer(tokenizer_name: &str, config_str: &str) {
    if let Err(e) = validate_tokenizer_name(tokenizer_name) {
        panic!("Invalid tokenizer name: {}, Details: {}", tokenizer_name, e);
    }

    let config: TokenizerConfig = toml::from_str(config_str).unwrap_or_report();
    if let Err(e) = config.validate() {
        panic!("Invalid tokenizer config, Details: {}", e);
    }

    pgrx::Spi::connect(|mut client| {
        let query = "INSERT INTO bm25_catalog.tokenizers (name, config) VALUES ($1, $2)";
        let args = Some(vec![
            (
                pgrx::PgBuiltInOids::TEXTOID.oid(),
                tokenizer_name.into_datum(),
            ),
            (pgrx::PgBuiltInOids::TEXTOID.oid(), config_str.into_datum()),
        ]);
        client.update(query, None, args).unwrap_or_report();
        if matches!(config.tokenizer, TokenizerKind::Unicode) {
            create_unicode_tokenizer_table(&mut client, tokenizer_name, &config);
        }
    });
}

#[pgrx::pg_extern(requires = ["tokenizer_table"])]
fn drop_tokenizer(tokenizer_name: &str) {
    if let Err(e) = validate_tokenizer_name(tokenizer_name) {
        panic!("Invalid tokenizer name: {}, Details: {}", tokenizer_name, e);
    }

    pgrx::Spi::connect(|mut client| {
        let query = "SELECT config FROM bm25_catalog.tokenizers WHERE name = $1";
        let args = Some(vec![(
            pgrx::PgBuiltInOids::TEXTOID.oid(),
            tokenizer_name.into_datum(),
        )]);
        let mut rows = client.select(query, None, args).unwrap_or_report();
        if rows.len() != 1 {
            panic!("Tokenizer not found");
        }

        let config: &str = rows
            .next()
            .unwrap()
            .get(1)
            .expect("no config value")
            .expect("no config value");
        let config: TokenizerConfig = toml::from_str(config).unwrap_or_report();
        if matches!(config.tokenizer, TokenizerKind::Unicode) {
            let table_name = format!("bm25_catalog.\"{}\"", tokenizer_name);
            let drop_table = format!("DROP TABLE IF EXISTS {}", table_name);
            client.update(&drop_table, None, None).unwrap_or_report();
            let drop_trigger = format!(
                "DROP TRIGGER IF EXISTS \"{}_trigger\" ON {}",
                tokenizer_name,
                config.table.unwrap()
            );
            client.update(&drop_trigger, None, None).unwrap_or_report();
        }

        let query = "DELETE FROM bm25_catalog.tokenizers WHERE name = $1";
        let args = Some(vec![(
            pgrx::PgBuiltInOids::TEXTOID.oid(),
            tokenizer_name.into_datum(),
        )]);
        client.update(query, None, args).unwrap_or_report();
    });
}

const TOKENIZER_RESERVED_NAMES: [&[u8]; 3] = [b"Bert", b"Tocken", b"tokenizers"];

// 1. It only contains ascii letters, numbers, and underscores.
// 2. It starts with a letter.
// 3. Its length cannot exceed NAMEDATALEN - 1
// 4. It is not a reserved name.
fn validate_tokenizer_name(name: &str) -> Result<(), String> {
    let name = name.as_bytes();
    for &b in name {
        if !b.is_ascii_alphanumeric() && b != b'_' {
            return Err(format!("Invalid character: {}", b as char));
        }
    }
    if !(1..=pgrx::pg_sys::NAMEDATALEN as usize - 1).contains(&name.len()) {
        return Err(format!(
            "Name length must be between 1 and {}",
            pgrx::pg_sys::NAMEDATALEN - 1
        ));
    }
    if !name[0].is_ascii_alphabetic() {
        return Err("Name must start with a letter".to_string());
    }
    if TOKENIZER_RESERVED_NAMES.contains(&name) {
        return Err("The name is reserved, please choose another name".to_string());
    }

    Ok(())
}

// 1. create word table
// 2. scan the text and split it into words and insert them into the word table
// 3. create a trigger to insert new words into the word table
fn create_unicode_tokenizer_table(
    client: &mut SpiClient<'_>,
    name: &str,
    config: &TokenizerConfig,
) {
    let table_name = format!("bm25_catalog.\"{}\"", name);
    let target_table = config.table.as_ref().unwrap();
    let column = config.column.as_ref().unwrap();

    let create_table = format!(
        r#"
        CREATE TABLE {} (
            id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            token TEXT NOT NULL UNIQUE
        );
        "#,
        table_name
    );
    client.update(&create_table, None, None).unwrap_or_report();

    let select_text = format!("SELECT {} FROM {}", column, target_table);
    let rows = client.select(&select_text, None, None).unwrap_or_report();
    let mut tokens = HashSet::new();
    for row in rows {
        let text: &str = row.get(1).unwrap_or_report().expect("no text value");
        let words = unicode_tokenizer_split(text);
        tokens.extend(words);
    }

    let insert_text = format!(
        r#"
        INSERT INTO {} (token) VALUES ($1)
        "#,
        table_name
    );
    for token in tokens {
        let args = Some(vec![(
            pgrx::PgBuiltInOids::TEXTOID.oid(),
            token.into_datum(),
        )]);
        client.update(&insert_text, None, args).unwrap_or_report();
    }

    let trigger = format!(
        r#"
        CREATE TRIGGER "{}_trigger"
        BEFORE INSERT OR UPDATE OF {}
        ON {}
        FOR EACH ROW
        EXECUTE FUNCTION unicode_tokenizer_insert_trigger('{}', '{}');
        "#,
        name, column, target_table, name, column
    );
    client.update(&trigger, None, None).unwrap_or_report();
}

fn unicode_tokenize(client: &SpiClient<'_>, text: &str, tokenizer_name: &str) -> Vec<u32> {
    let tokens = unicode_tokenizer_split(text);
    let query = format!(
        "SELECT id, token FROM bm25_catalog.\"{}\" WHERE token = ANY($1)",
        tokenizer_name
    );
    let args = Some(vec![(
        pgrx::PgBuiltInOids::TEXTARRAYOID.oid(),
        tokens.clone().into_datum(),
    )]);
    let rows = client.select(&query, None, args).unwrap_or_report();

    let mut token_map = HashMap::new();
    for row in rows {
        let id: i32 = row.get(1).unwrap_or_report().expect("no id value");
        let id = u32::try_from(id).expect("id is not a valid u32");
        let token: String = row.get(2).unwrap_or_report().expect("no token value");
        token_map.insert(token, id);
    }

    tokens
        .into_iter()
        .filter_map(|token| token_map.get(&token).copied())
        .collect()
}

#[pgrx::pg_extern(stable, strict, parallel_safe, requires = ["tokenizer_table"])]
pub fn tokenize(content: &str, tokenizer_name: &str) -> Bm25VectorOutput {
    let term_ids = match tokenizer_name {
        "Bert" => BERT_TOKENIZER.encode(content),
        "Tocken" => TOCKENIZER.encode(content),
        _ => custom_tokenize(content, tokenizer_name),
    };
    Bm25VectorOutput::from_ids(&term_ids)
}

fn custom_tokenize(text: &str, tokenizer_name: &str) -> Vec<u32> {
    pgrx::Spi::connect(|client| {
        let query = "SELECT config FROM bm25_catalog.tokenizers WHERE name = $1";
        let args = Some(vec![(
            pgrx::PgBuiltInOids::TEXTOID.oid(),
            tokenizer_name.into_datum(),
        )]);
        let mut rows = client.select(query, None, args).unwrap_or_report();
        if rows.len() != 1 {
            panic!("Tokenizer not found");
        }

        let config: &str = rows
            .next()
            .unwrap()
            .get(1)
            .expect("no config value")
            .expect("no config value");
        let config: TokenizerConfig = toml::from_str(config).unwrap_or_report();
        match config.tokenizer {
            TokenizerKind::Bert => BERT_TOKENIZER.encode(text),
            TokenizerKind::Tocken => TOCKENIZER.encode(text),
            TokenizerKind::Unicode => unicode_tokenize(&client, text, tokenizer_name),
        }
    })
}

#[pg_trigger]
fn unicode_tokenizer_set_target_column_trigger<'a>(
    trigger: &'a pgrx::PgTrigger<'a>,
) -> Result<Option<PgHeapTuple<'a, impl WhoAllocated>>, ()> {
    let mut new = trigger.new().expect("new tuple is missing").into_owned();
    let tg_argv = trigger.extra_args().expect("trigger arguments are missing");
    if tg_argv.len() != 3 {
        panic!("Invalid trigger arguments");
    }
    let tokenizer_name = &tg_argv[0];
    let source_column = &tg_argv[1];
    let target_column = &tg_argv[2];

    let source = new
        .get_by_name::<&str>(source_column)
        .expect("source column is missing");
    let Some(source) = source else {
        return Ok(Some(new));
    };

    let target = tokenize(source, tokenizer_name);
    new.set_by_name(target_column, target)
        .expect("set target column failed");
    Ok(Some(new))
}

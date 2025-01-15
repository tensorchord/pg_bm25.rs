# Tokenizer

Currently, we support the following tokenizers:

- `Bert`: default uncased BERT tokenizer.
- `Tocken`: a Unicode tokenizer pre-trained on wiki-103-raw with `min_freq=10`.
- `Unicode`: a Unicode tokenizer that will be trained on your data.

## Usage

### Pre-trained Tokenizer

`Bert` and `Tocken` are pre-trained tokenizers. You can use them directly by calling the `tokenize` function.

```sql
SELECT tokenize('A quick brown fox jumps over the lazy dog.', 'Bert');  -- or 'Tocken'
-- {2058:1, 2474:1, 2829:1, 3899:1, 4248:1, 4419:1, 5376:1, 5831:1}
```

### Train on Your Data

`Unicode` will be trained on your data during the document tokenization. You can use this function with/without the trigger:

- with trigger (convenient but slower)

```sql
CREATE TABLE corpus (id SERIAL, text TEXT, embedding bm25vector);
SELECT create_unicode_tokenizer_and_trigger('test_token', 'corpus', 'text', 'embedding');
INSERT INTO corpus (text) VALUES ('PostgreSQL is a powerful, open-source object-relational database system.'); -- insert text to the table
CREATE INDEX corpus_embedding_bm25 ON corpus USING bm25 (embedding bm25_ops);
SELECT id, text, embedding <&> to_bm25query('corpus_embedding_bm25', 'PostgreSQL', 'test_token') AS rank
    FROM corpus
    ORDER BY rank
    LIMIT 10;
```

- without trigger (faster but need to call the `tokenize` function manually)

```sql
CREATE TABLE corpus (id SERIAL, text TEXT, embedding bm25vector);
INSERT INTO corpus (text) VALUES ('PostgreSQL is a powerful, open-source object-relational database system.'); -- insert text to the table
SELECT create_tokenizer('test_token', $$
tokenizer = 'Unicode'
table = 'corpus'
column = 'text'
$$);
UPDATE corpus SET embedding = tokenize(text, 'test_token');
CREATE INDEX corpus_embedding_bm25 ON corpus USING bm25 (embedding bm25_ops);
SELECT id, text, embedding <&> to_bm25query('corpus_embedding_bm25', 'PostgreSQL', 'test_token') AS rank
    FROM corpus
    ORDER BY rank
    LIMIT 10;
```

## Configuration

We utilize [`TOML`](https://toml.io/en/) to configure the tokenizer. You can specify the tokenizer type and the table/column to train on.

Here is what each field means:

| Field     | Type   | Description                                          |
| --------- | ------ | ---------------------------------------------------- |
| tokenizer | String | The tokenizer type (`Bert`, `Tocken`, or `Unicode`). |
| table     | String | The table name to train on for Unicode tokenizer.    |
| column    | String | The column name to train on for Unicode tokenizer.   |

## Note

- `tokenizer_name` is case-sensitive. Make sure to use the exact name when calling the `tokenize` function.
- `tokenizer_name` can only contain alphanumeric characters and underscores, and it must start with an alphabet.
- `tokenizer_name` is unique. You cannot create two tokenizers with the same name.

## Contribution

To create another tokenizer that is pre-trained on your data, you can follow the steps below:

1. update `TOKENIZER_RESERVED_NAMES`, `create_tokenizer`, `drop_tokenizer`, and `tokenize` functions in the [`token.rs`](src/token.rs).
2. (optional) pre-trained data can be stored under the [tokenizer](./tokenizer/) directory.

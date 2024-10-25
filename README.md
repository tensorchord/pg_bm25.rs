# pg_bm25.rs (WIP)

a postgresql extension for bm25 ranking algorithm, inspired by [tantivy](https://github.com/quickwit-oss/tantivy)

## Example

```sql
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    passage TEXT
);

INSERT INTO documents (passage) VALUES 
('PostgreSQL is a powerful, open-source object-relational database system. It has over 15 years of active development.'),
('Full-text search is a technique for searching in plain-text documents or textual database fields. PostgreSQL supports this with tsvector.'),
('BM25 is a ranking function used by search engines to estimate the relevance of documents to a given search query.'),
('PostgreSQL provides many advanced features like full-text search, window functions, and more.'),
('Search and ranking in databases are important in building effective information retrieval systems.'),
('The BM25 ranking algorithm is derived from the probabilistic retrieval framework.'),
('Full-text search indexes documents to allow fast text queries. PostgreSQL supports this through its GIN and GiST indexes.'),
('The PostgreSQL community is active and regularly improves the database system.'),
('Relational databases such as PostgreSQL can handle both structured and unstructured data.'),
('Effective search ranking algorithms, such as BM25, improve search results by understanding relevance.');

ALTER TABLE documents ADD COLUMN embedding bm25vector;

UPDATE documents SET embedding = tokenize(passage);

CREATE INDEX documents_embedding_bm25 ON documents USING bm25 (embedding bm25_ops);

SELECT id, passage, embedding <&> to_bm25query('documents_embedding_bm25', 'PostgreSQL') AS rank
FROM documents
ORDER BY rank
LIMIT 10;
```

## Installation

1. Setup development environment.

You can follow the docs about [`pgvecto.rs`](https://docs.pgvecto.rs/developers/development.html).

2. Install the extension.

```sh
cargo pgrx install --sudo --release
```

3. Configure your PostgreSQL by modifying the `shared_preload_libraries` and `search_path` to include the extension.

```sh
psql -U postgres -c 'ALTER SYSTEM SET shared_preload_libraries = "pg_bm25.so"'
psql -U postgres -c 'ALTER SYSTEM SET search_path TO "$user", public, bm25_catalog'
# You need restart the PostgreSQL cluster to take effects.
sudo systemctl restart postgresql.service   # for pg_bm25.rs running with systemd
```

4. Connect to the database and enable the extension.

```sql
DROP EXTENSION IF EXISTS pg_bm25;
CREATE EXTENSION pg_bm25;
```

## Reference

### Data Types

- `bm25vector`: A vector type for storing BM25 tokenized text.
- `bm25query`: A query type for BM25 ranking.

### Functions

- `tokenize(text) RETURNS bm25vector`: Tokenize the input text into a BM25 vector.
- `to_bm25query(index_name regclass, query text) RETURNS bm25query`: Convert the input text into a BM25 query.
- `bm25vector <&> bm25query RETURNS float4`: Calculate the **negative** BM25 score between the BM25 vector and query.

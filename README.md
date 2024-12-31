# VectorChord-BM25

A PostgreSQL extension for bm25 ranking algorithm. We implemented the Block-WeakAnd Algorithms for BM25 ranking inside PostgreSQL. This extension is currently in **alpha** stage and not recommended for production use. We're still iterating on the API and performance. The interface may change in the future.

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

## Performance Benchmark

We used datasets are from [xhluca/bm25-benchmarks](https://github.com/xhluca/bm25-benchmarks) and compare the results with ElasticSearch and Lucene. The QPS reflects the query efficiency with the index structure. And the NDCG@10 reflects the ranking quality of the search engine, which is totally based on the tokenizer. This means we can achieve the same ranking quality as ElasticSearch and Lucene if using the exact same tokenizer. 

### QPS Result

| Dataset          | VectorChord-BM25 | ElasticSearch |
| ---------------- | ---------------- | ------------- |
| trec-covid       | 28.38            | 27.31         |
| webis-touche2020 | 38.57            | 32.05         |

### NDCG@10 Result

| Dataset          | VectorChord-BM25 | ElasticSearch | Lucene |
| ---------------- | ---------------- | ------------- | ------ |
| trec-covid       | 67.67            | 68.80         | 61.0   |
| webis-touche2020 | 31.0             | 34.70         | 33.2   |

## Installation

1. Setup development environment.

You can follow the docs about [`pgvecto.rs`](https://docs.pgvecto.rs/developers/development.html).

2. Install the extension.

```sh
cargo pgrx install --sudo --release
```

3. Configure your PostgreSQL by modifying `search_path` to include the extension.

```sh
psql -U postgres -c 'ALTER SYSTEM SET search_path TO "$user", public, bm25_catalog'
# You need restart the PostgreSQL cluster to take effects.
sudo systemctl restart postgresql.service   # for vchord_bm25.rs running with systemd
```

4. Connect to the database and enable the extension.

```sql
DROP EXTENSION IF EXISTS vchord_bm25;
CREATE EXTENSION vchord_bm25;
```

## Limitation
- We currently only support bert-uncased tokenizer, with Porter stemmer and split the text with space. Will extend more tokenizer configurations in the future.
- The index will return up to `bm25_catalog.bm25_limit` results to PostgreSQL. Users need to adjust the `bm25_catalog.bm25_limit` for more results when using larger limit values or stricter filter conditions.

## Reference

### Data Types

- `bm25vector`: A vector type for storing BM25 tokenized text.
- `bm25query`: A query type for BM25 ranking.

### Functions

- `tokenize(text) RETURNS bm25vector`: Tokenize the input text into a BM25 vector.
- `to_bm25query(index_name regclass, query text) RETURNS bm25query`: Convert the input text into a BM25 query.
- `bm25vector <&> bm25query RETURNS float4`: Calculate the **negative** BM25 score between the BM25 vector and query.

### GUCs

- `bm25_catalog.bm25_limit (integer)`: The maximum number of documents to return in a search. Default is 1, minimum is 1, and maximum is 65535.
- `bm25_catalog.enable_index (boolean)`: Whether to enable the bm25 index. Default is false.
- `bm25_catalog.segment_growing_max_page_size (integer)`: The maximum page count of the growing segment. When the size of the growing segment exceeds this value, the segment will be sealed into a read-only segment. Default is 1, minimum is 1, and maximum is 1,000,000.

## License

This software is licensed under a dual license model:

1. **GNU Affero General Public License v3 (AGPLv3)**: You may use, modify, and distribute this software under the terms of the AGPLv3.

2. **Elastic License v2 (ELv2)**: You may also use, modify, and distribute this software under the Elastic License v2, which has specific restrictions.

You may choose either license based on your needs. We welcome any commercial collaboration or support, so please email us <vectorchord-inquiry@tensorchord.ai> with any questions or requests regarding the licenses.
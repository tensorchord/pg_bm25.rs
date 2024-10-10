# pg_bm25.rs (WIP)

a postgresql extension for bm25 ranking algorithm, inspired by [tantivy](https://github.com/quickwit-oss/tantivy)

## Example

```sql
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    body TEXT
);

INSERT INTO documents (body) VALUES 
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

CREATE INDEX documents_body_bm25 ON documents USING bm25 (body bm25_ops);

SELECT id, body, body <&> to_bm25query('documents_body_bm25', 'Postgresql') AS rank
FROM documents
ORDER BY rank
LIMIT 10;
```

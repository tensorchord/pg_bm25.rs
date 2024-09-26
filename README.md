# pg_bm25.rs (WIP)

a postgresql extension for bm25 ranking algorithm, inspired by [tantivy](https://github.com/quickwit-oss/tantivy)

## Example

```sql
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    title TEXT,
    body TEXT
);

INSERT INTO documents (title, body) VALUES 
('Document 1', 'PostgreSQL is an advanced open-source relational database.'),
('Document 2', 'This document covers advanced topics in PostgreSQL full-text search.'),
('Document 3', 'Text search in PostgreSQL is based on bm25 ranking.');

CREATE INDEX documents_body_bm25 ON documents USING bm25 (body bm25_ops);

SELECT id, title, body <&> to_bm25query('documents_body_bm25', 'PostgreSQL') AS rank
FROM documents
ORDER BY rank
LIMIT 10;
```

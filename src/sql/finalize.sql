CREATE TYPE bm25vector (
    INPUT = _bm25catalog_bm25vector_in,
    OUTPUT = _bm25catalog_bm25vector_out,
    RECEIVE = _bm25catalog_bm25vector_recv,
    SEND = _bm25catalog_bm25vector_send,
    STORAGE = EXTERNAL,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT = double
);

CREATE TYPE bm25query AS (
    index_oid regclass,
    query_vector bm25vector
);

CREATE FUNCTION to_bm25query(index_oid regclass, query_str text) RETURNS bm25query
    IMMUTABLE STRICT PARALLEL SAFE LANGUAGE sql AS $$
        SELECT index_oid, tokenize(query_str);
    $$;

CREATE ACCESS METHOD bm25 TYPE INDEX HANDLER _bm25_amhandler;
COMMENT ON ACCESS METHOD bm25 IS 'vchord bm25 index access method';

CREATE OPERATOR pg_catalog.<&> (
    PROCEDURE = search_bm25query,
    LEFTARG = bm25vector,
    RIGHTARG = bm25query
);

CREATE OPERATOR FAMILY bm25_ops USING bm25;

CREATE OPERATOR CLASS bm25_ops FOR TYPE bm25vector USING bm25 FAMILY bm25_ops AS
    OPERATOR 1 pg_catalog.<&>(bm25vector, bm25query) FOR ORDER BY float_ops;

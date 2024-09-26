CREATE ACCESS METHOD bm25 TYPE INDEX HANDLER _bm25_amhandler;
COMMENT ON ACCESS METHOD bm25 IS 'pg_bm25 index access method';

CREATE OPERATOR pg_catalog.<&> (
    PROCEDURE = search_bm25query,
    LEFTARG = text,
    RIGHTARG = bm25query
);

CREATE OPERATOR FAMILY bm25_ops USING bm25;

CREATE OPERATOR CLASS bm25_ops FOR TYPE text USING bm25 FAMILY bm25_ops AS
    OPERATOR 1 pg_catalog.<&>(text, bm25query) FOR ORDER BY float_ops;

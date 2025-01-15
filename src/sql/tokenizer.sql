CREATE TABLE bm25_catalog.tokenizers (
    name TEXT NOT NULL UNIQUE PRIMARY KEY,
    config TEXT NOT NULL
);

CREATE FUNCTION unicode_tokenizer_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
    tokenizer_name TEXT := TG_ARGV[0];
    target_column TEXT := TG_ARGV[1];
BEGIN
    EXECUTE format('
    WITH new_tokens AS (
        SELECT unnest(unicode_tokenizer_split($1.%I)) AS token
    ),
    to_insert AS (
        SELECT token FROM new_tokens
        WHERE NOT EXISTS (
            SELECT 1 FROM bm25_catalog.%I WHERE token = new_tokens.token
        )
    )
    INSERT INTO bm25_catalog.%I (token) SELECT token FROM to_insert ON CONFLICT (token) DO NOTHING', target_column, tokenizer_name, tokenizer_name) USING NEW;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION create_unicode_tokenizer_and_trigger(tokenizer_name TEXT, table_name TEXT, source_column TEXT, target_column TEXT)
RETURNS VOID AS $body$
BEGIN
    EXECUTE format('SELECT create_tokenizer(%L, $$
        tokenizer = ''Unicode''
        table = %L
        column = %L
        $$)', tokenizer_name, table_name, source_column);
    EXECUTE format('UPDATE %I SET %I = tokenize(%I, %L)', table_name, target_column, source_column, tokenizer_name);
    EXECUTE format('CREATE TRIGGER "%s_trigger_insert" BEFORE INSERT OR UPDATE OF %I ON %I FOR EACH ROW EXECUTE FUNCTION unicode_tokenizer_set_target_column_trigger(%L, %I, %I)', tokenizer_name, source_column, table_name, tokenizer_name, source_column, target_column);
END;
$body$ LANGUAGE plpgsql;

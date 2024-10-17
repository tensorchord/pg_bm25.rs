use pgrx::PostgresType;
use serde::{Deserialize, Serialize};

use crate::{
    page::{page_read, MetaPageData, METAPAGE_BLKNO},
    postings::{TermDictReader, TermInfoReader},
    weight::{idf, Bm25Weight},
};

#[repr(C, align(8))]
#[derive(Debug, Serialize, Deserialize, PostgresType)]
pub struct Bm25Query {
    pub index_oid: pgrx::pg_sys::Oid,
    pub query_str: String, // TODO: store str internally
}

#[pgrx::pg_extern(
    immutable,
    strict,
    parallel_safe,
    sql = "
    CREATE FUNCTION to_bm25query(index_oid regclass, query_str text) RETURNS bm25query
    IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';"
)]
pub fn to_bm25query(index_oid: pgrx::pg_sys::Oid, query_str: String) -> Bm25Query {
    Bm25Query {
        index_oid,
        query_str,
    }
}

#[pgrx::pg_extern(
    stable,
    strict,
    parallel_safe,
    sql = "
    CREATE FUNCTION search_bm25query(target_doc text, query bm25query) RETURNS real
    STABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';"
)]
pub fn search_bm25query(target_doc: &str, query: Bm25Query) -> f32 {
    let index =
        unsafe { pgrx::pg_sys::index_open(query.index_oid, pgrx::pg_sys::AccessShareLock as _) };
    let meta = {
        let page = page_read(index, METAPAGE_BLKNO);
        unsafe { (page.content.as_ptr() as *const MetaPageData).read() }
    };

    let tokens = crate::token::BERT_BASE_UNCASED
        .encode(query.query_str, false)
        .expect("failed to tokenize");
    let tokens = tokens.get_tokens();
    let target_tokens = crate::token::BERT_BASE_UNCASED
        .encode(target_doc, false)
        .expect("failed to tokenize");
    let target_tokens = target_tokens.get_tokens();
    let len = target_tokens.len().try_into().unwrap();

    let term_dict_reader = TermDictReader::new(index, meta.term_dict_blkno).unwrap();
    let term_info_reader = TermInfoReader::new(index, meta.term_info_blkno);
    let mut scores = 0.0;
    for token in tokens {
        let tf = target_tokens
            .iter()
            .filter(|&t| t == token)
            .count()
            .try_into()
            .unwrap();
        let term_id = term_dict_reader.get(token.as_ref()).unwrap();
        let term_info = term_info_reader.read(term_id);
        let idf = idf(meta.doc_cnt, term_info.docs);
        let bm25_weight = Bm25Weight::new(idf, meta.avg_dl);
        let score = bm25_weight.score(len, tf);
        scores += score;
    }

    unsafe {
        pgrx::pg_sys::index_close(index, pgrx::pg_sys::AccessShareLock as _);
    }
    scores * -1.0
}

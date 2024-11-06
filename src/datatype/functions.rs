use std::{collections::BTreeMap, num::NonZero};

use crate::{
    page::{page_read, METAPAGE_BLKNO},
    segments::meta::MetaPageData,
    term_info::TermInfoReader,
    weight::bm25_score_batch,
};

use super::{
    memory_bm25vector::{Bm25VectorInput, Bm25VectorOutput},
    Bm25VectorBorrowed,
};

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
pub fn tokenize(text: &str) -> Bm25VectorOutput {
    let term_ids = crate::token::tokenize(text);
    let mut map: BTreeMap<u32, u32> = BTreeMap::new();
    for term_id in term_ids {
        *map.entry(term_id).or_insert(0) += 1;
    }
    let mut doc_len: u32 = 0;
    let mut indexes = Vec::with_capacity(map.len());
    let mut values = Vec::with_capacity(map.len());
    for (index, value) in map {
        indexes.push(index);
        values.push(value);
        doc_len = doc_len.checked_add(value).expect("overflow");
    }
    let vector = unsafe { Bm25VectorBorrowed::new_unchecked(doc_len, &indexes, &values) };
    Bm25VectorOutput::new(vector)
}

#[pgrx::pg_extern(stable, strict, parallel_safe)]
pub fn search_bm25query(
    target_vector: Bm25VectorInput,
    query: pgrx::composite_type!("bm25query"),
) -> f32 {
    let index_oid: pgrx::pg_sys::Oid = query
        .get_by_index(NonZero::new(1).unwrap())
        .unwrap()
        .unwrap();
    let query_vector: Bm25VectorOutput = query
        .get_by_index(NonZero::new(2).unwrap())
        .unwrap()
        .unwrap();
    let query_vector = query_vector.borrow();
    let target_vector = target_vector.borrow();

    let index =
        unsafe { pgrx::PgRelation::with_lock(index_oid, pgrx::pg_sys::AccessShareLock as _) };
    let meta = {
        let page = page_read(index.as_ptr(), METAPAGE_BLKNO);
        unsafe { &*(page.data().as_ptr() as *const MetaPageData) }
    };

    let term_info_reader = TermInfoReader::new(index.as_ptr(), meta.term_info_blkno);
    let avgdl = meta.doc_term_cnt as f32 / meta.doc_cnt as f32;
    let scores = bm25_score_batch(
        meta.doc_cnt,
        avgdl,
        &term_info_reader,
        target_vector,
        query_vector,
    );

    scores * -1.0
}

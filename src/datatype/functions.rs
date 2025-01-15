use std::num::NonZero;

use crate::{
    page::{page_read, METAPAGE_BLKNO},
    segment::{meta::MetaPageData, term_stat::TermStatReader},
    weight::bm25_score_batch,
};

use super::memory_bm25vector::{Bm25VectorInput, Bm25VectorOutput};

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

    let term_stat_reader = TermStatReader::new(index.as_ptr(), meta);
    let avgdl = meta.avgdl();
    let scores = bm25_score_batch(
        meta.doc_cnt,
        avgdl,
        &term_stat_reader,
        target_vector,
        query_vector,
    );

    scores * -1.0
}

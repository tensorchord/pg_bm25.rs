use std::num::NonZero;

use pgrx::{prelude::PgHeapTuple, FromDatum};

use crate::{
    algorithm::block_wand::{block_wand, block_wand_single, SealedScorer},
    datatype::{Bm25VectorBorrowed, Bm25VectorOutput},
    guc::BM25_LIMIT,
    page::{page_read, METAPAGE_BLKNO},
    segment::{
        delete::DeleteBitmapReader, field_norm::FieldNormReader, growing::GrowingSegmentReader,
        meta::MetaPageData, payload::PayloadReader, sealed::SealedSegmentReader,
        term_stat::TermStatReader,
    },
    utils::topk_computer::TopKComputer,
    weight::{bm25_score_batch, idf, Bm25Weight},
};

enum Scanner {
    Initial,
    Waiting {
        query_index: pgrx::PgRelation,
        query_vector: Bm25VectorOutput,
    },
    Scanned {
        results: Vec<u64>,
    },
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambeginscan(
    index: pgrx::pg_sys::Relation,
    n_keys: std::os::raw::c_int,
    n_orderbys: std::os::raw::c_int,
) -> pgrx::pg_sys::IndexScanDesc {
    use pgrx::memcxt::PgMemoryContexts::CurrentMemoryContext;

    assert!(n_keys == 0, "it doesn't support WHERE clause");
    assert!(n_orderbys == 1, "it only supports one ORDER BY clause");
    let scan = pgrx::pg_sys::RelationGetIndexScan(index, n_keys, n_orderbys);
    (*scan).opaque = CurrentMemoryContext
        .leak_and_drop_on_delete(Scanner::Initial)
        .cast();
    scan
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amrescan(
    scan: pgrx::pg_sys::IndexScanDesc,
    _keys: pgrx::pg_sys::ScanKey,
    _n_keys: std::os::raw::c_int,
    orderbys: pgrx::pg_sys::ScanKey,
    _n_orderbys: std::os::raw::c_int,
) {
    assert!(!orderbys.is_null());
    std::ptr::copy(orderbys, (*scan).orderByData, (*scan).numberOfOrderBys as _);
    let data = (*scan).orderByData;
    let value = (*data).sk_argument;
    let is_null = ((*data).sk_flags & pgrx::pg_sys::SK_ISNULL as i32) != 0;
    let bm25_query = PgHeapTuple::from_datum(value, is_null).unwrap();
    let index_oid = bm25_query
        .get_by_index(NonZero::new(1).unwrap())
        .unwrap()
        .unwrap();
    let query_vector = bm25_query
        .get_by_index(NonZero::new(2).unwrap())
        .unwrap()
        .unwrap();

    let scanner = (*scan).opaque.cast::<Scanner>().as_mut().unwrap();
    *scanner = Scanner::Waiting {
        query_index: pgrx::PgRelation::with_lock(index_oid, pgrx::pg_sys::AccessShareLock as _),
        query_vector,
    };
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amgettuple(
    scan: pgrx::pg_sys::IndexScanDesc,
    direction: pgrx::pg_sys::ScanDirection::Type,
) -> bool {
    if direction != pgrx::pg_sys::ScanDirection::ForwardScanDirection {
        pgrx::error!("bm25 index without a forward scan direction is not supported");
    }

    let scanner = unsafe { (*scan).opaque.cast::<Scanner>().as_mut().unwrap() };
    let results = match scanner {
        Scanner::Initial => return false,
        Scanner::Waiting {
            query_index,
            query_vector,
        } => {
            let results = scan_main(query_index.as_ptr(), query_vector.borrow());
            *scanner = Scanner::Scanned { results };
            let Scanner::Scanned { results } = scanner else {
                unreachable!()
            };
            results
        }
        Scanner::Scanned { results } => results,
    };

    if let Some(tid) = results.pop() {
        pgrx::itemptr::u64_to_item_pointer(tid, &mut (*scan).xs_heaptid);
        (*scan).xs_recheckorderby = false;
        (*scan).xs_recheck = false;
        true
    } else {
        false
    }
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amendscan(scan: pgrx::pg_sys::IndexScanDesc) {
    let scanner = unsafe { (*scan).opaque.cast::<Scanner>().as_mut().unwrap() };
    *scanner = Scanner::Initial;
}

// return top-k results
unsafe fn scan_main(index: pgrx::pg_sys::Relation, query_vector: Bm25VectorBorrowed) -> Vec<u64> {
    let page = page_read(index, METAPAGE_BLKNO);
    let meta: &MetaPageData = page.as_ref();
    let avgdl = meta.avgdl();

    let mut computer = TopKComputer::new(BM25_LIMIT.get() as _);
    let delete_bitmap_reader = DeleteBitmapReader::new(index, meta.delete_bitmap_blkno);

    let term_stat_reader = TermStatReader::new(index, meta.term_stat_blkno);
    if let Some(growing) = meta.growing_segment.as_ref() {
        let reader = GrowingSegmentReader::new(index, growing);
        let mut doc_id = meta.sealed_doc_id;
        let mut iter = reader.into_iter();
        while let Some(vector) = iter.next() {
            if !delete_bitmap_reader.is_delete(doc_id) {
                let score =
                    bm25_score_batch(meta.doc_cnt, avgdl, &term_stat_reader, vector, query_vector);
                computer.push(score, doc_id);
            }
            doc_id += 1;
        }
    }

    let fieldnorm_reader = FieldNormReader::new(index, meta.field_norm_blkno);
    let bm25_weight = query_vector
        .indexes()
        .iter()
        .zip(query_vector.values())
        .map(|(&term_id, &term_tf)| {
            let term_cnt = term_stat_reader.read(term_id);
            let idf = idf(meta.doc_cnt, term_cnt);
            Bm25Weight::new(term_tf, idf, avgdl)
        })
        .collect::<Vec<_>>();

    for &sealed_data in meta.sealed_segment() {
        let sealed_reader = SealedSegmentReader::new(index, sealed_data);
        let term_ids = query_vector.indexes();
        let mut scorers = Vec::new();

        for i in 0..term_ids.len() {
            let term_id = term_ids[i];
            if let Some(posting_reader) = sealed_reader.get_postings(term_id) {
                let weight = bm25_weight[i];
                scorers.push(SealedScorer {
                    posting: posting_reader,
                    weight,
                    max_score: weight.max_score(),
                });
            }
        }

        if scorers.len() == 1 {
            block_wand_single(
                scorers.into_iter().next().unwrap(),
                &fieldnorm_reader,
                &delete_bitmap_reader,
                &mut computer,
            );
        } else {
            block_wand(
                scorers,
                &fieldnorm_reader,
                &delete_bitmap_reader,
                &mut computer,
            );
        }
    }

    let payload_reader = PayloadReader::new(index, meta.payload_blkno);
    computer
        .to_sorted_slice()
        .iter()
        .map(|(_, doc_id)| payload_reader.read(*doc_id))
        .collect()
}

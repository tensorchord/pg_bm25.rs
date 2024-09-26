use std::collections::HashSet;

use pgrx::FromDatum;

use crate::{
    bm25query::Bm25Query,
    bm25weight::{idf, Bm25Weight},
    field_norm::{id_to_fieldnorm, FieldNormReader},
    guc::BM25_LIMIT,
    page::{page_get_contents, MetaPageData, METAPAGE_BLKNO},
    payload::PayloadReader,
    postings::{InvertedReader, Posting},
    utils::topk_computer::TopKComputer,
};

enum Scanner {
    Initial,
    Waiting {
        query_index: pgrx::PgRelation,
        query_str: String,
    },
    Scanned {
        cached_results: Vec<u64>,
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
    let bm25_query = Bm25Query::from_datum(value, is_null).unwrap();

    let scanner = (*scan).opaque.cast::<Scanner>().as_mut().unwrap();
    *scanner = Scanner::Waiting {
        query_index: pgrx::PgRelation::with_lock(
            bm25_query.index_oid,
            pgrx::pg_sys::AccessShareLock as _,
        ),
        query_str: bm25_query.query_str,
    };
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amgettuple(
    scan: pgrx::pg_sys::IndexScanDesc,
    direction: pgrx::pg_sys::ScanDirection::Type,
) -> bool {
    assert!(direction == pgrx::pg_sys::ScanDirection::ForwardScanDirection, "only support forward scan");
    let scanner = unsafe { (*scan).opaque.cast::<Scanner>().as_mut().unwrap() };
    let results = match scanner {
        Scanner::Initial => return false,
        Scanner::Waiting {
            query_index,
            query_str,
        } => {
            let results = scan_main(query_index.as_ptr(), &query_str);
            *scanner = Scanner::Scanned {
                cached_results: results,
            };
            let Scanner::Scanned { cached_results } = scanner else {
                unreachable!()
            };
            cached_results
        }
        Scanner::Scanned { cached_results } => cached_results,
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

// return reversed top-k results
unsafe fn scan_main(index: pgrx::pg_sys::Relation, query_str: &str) -> Vec<u64> {
    let meta = unsafe {
        let meta_buffer = pgrx::pg_sys::ReadBuffer(index, METAPAGE_BLKNO);
        pgrx::pg_sys::LockBuffer(meta_buffer, pgrx::pg_sys::BUFFER_LOCK_SHARE as _);
        let meta_page = pgrx::pg_sys::BufferGetPage(meta_buffer);
        let meta_data: *mut MetaPageData = page_get_contents(meta_page);
        let meta_data = (*meta_data).clone();
        pgrx::pg_sys::UnlockReleaseBuffer(meta_buffer);
        meta_data
    };

    let inverted_reader = InvertedReader::new(index, &meta).unwrap();
    let tokens = crate::token::BERT_BASE_UNCASED
        .encode_fast(query_str, false)
        .expect("failed to tokenize");
    let tokens = tokens.get_tokens();

    let posting_readers = tokens
        .iter()
        .map(|token| inverted_reader.get_posting_reader(token.as_ref()).unwrap())
        .collect::<Vec<_>>();

    let mut idf_sum = 0.0;
    for r in &posting_readers {
        idf_sum += idf(meta.doc_cnt, r.doc_cnt());
    }
    let bm25_weight = Bm25Weight::new(idf_sum, meta.avg_dl);

    let fieldnorm_reader = FieldNormReader::new(index, meta.field_norms_blkno);
    let mut results;
    if posting_readers.len() == 1 {
        let scorer = PostingScorer {
            postings: posting_readers[0].get_posting(),
            fieldnorm_reader: &fieldnorm_reader,
            weight: bm25_weight,
        };
        results = block_wand_single(scorer);
    } else {
        let scorers = posting_readers
            .iter()
            .map(|r| PostingScorer {
                postings: r.get_posting(),
                fieldnorm_reader: &fieldnorm_reader,
                weight: bm25_weight,
            })
            .collect::<Vec<_>>();
        results = block_wand(scorers);
    }

    let payload_reader = PayloadReader::new(index, meta.payload_blkno);
    results
        .to_sorted_slice()
        .iter()
        .map(|(_, doc_id)| payload_reader.read(*doc_id))
        .collect()
}

struct PostingScorer<'a> {
    postings: Posting<'a>,
    fieldnorm_reader: &'a FieldNormReader,
    weight: Bm25Weight,
}

fn block_wand_single(mut scorer: PostingScorer) -> TopKComputer {
    let mut results = TopKComputer::new(BM25_LIMIT.get() as usize);
    'outer: loop {
        while scorer.postings.block_max_score(&scorer.weight) < results.threshold() {
            if !scorer.postings.advance_block() {
                break 'outer;
            }
        }
        scorer.postings.decode_block();
        loop {
            let doc_id = scorer.postings.doc_id();
            let tf = scorer.postings.term_freq();
            let fieldnorm_id = scorer.fieldnorm_reader.read(doc_id);
            let fieldnorm = id_to_fieldnorm(fieldnorm_id);
            let score = scorer.weight.score(tf, fieldnorm);
            results.push(score, scorer.postings.doc_id());
            if !scorer.postings.advance_cur() {
                break;
            }
        }
        if !scorer.postings.advance_block() {
            break;
        }
    }
    results
}

// TODO: use optimized block wand algorithm for multiple terms
fn block_wand(scorers: Vec<PostingScorer>) -> TopKComputer {
    let mut results = TopKComputer::new(BM25_LIMIT.get() as usize);
    let mut visited = HashSet::new();
    for mut scorer in scorers {
        'outer: loop {
            while scorer.postings.block_max_score(&scorer.weight) < results.threshold() {
                if !scorer.postings.advance_block() {
                    break 'outer;
                }
            }
            scorer.postings.decode_block();
            loop {
                let doc_id = scorer.postings.doc_id();
                if visited.insert(doc_id) {
                    let tf = scorer.postings.term_freq();
                    let fieldnorm_id = scorer.fieldnorm_reader.read(doc_id);
                    let fieldnorm = id_to_fieldnorm(fieldnorm_id);
                    let score = scorer.weight.score(tf, fieldnorm);
                    results.push(score, scorer.postings.doc_id());
                }
                if !scorer.postings.advance_cur() {
                    break;
                }
            }
            if !scorer.postings.advance_block() {
                break;
            }
        }
    }
    results
}

// struct PostingScorerWithMax<'a> {
//     postings: Posting<'a>,
//     fieldnorm_reader: &'a FieldNormReader,
//     weight: Bm25Weight,
//     max_score: f32,
// }

// fn block_wand(mut scorers: Vec<PostingScorerWithMax>) -> TopKComputer {
//     let mut results = TopKComputer::new(BM25_LIMIT.get() as usize);

//     for s in &mut scorers {
//         s.postings.decode_block();
//     }
//     scorers.sort_by_key(|s| s.postings.doc_id());

//     while let Some((before_pivot_len, pivot_len, pivot_doc)) =
//         find_pivot_doc(&scorers, results.threshold())
//     {
//         let block_max_score_upperbound: f32 = scorers[..pivot_len]
//             .iter_mut()
//             .map(|scorer| {
//                 scorer.postings.shallow_seek(pivot_doc);
//                 scorer.postings.block_max_score(&scorer.weight)
//             })
//             .sum();
//     }

//     loop {}
// }

// fn find_pivot_doc(scorers: &[PostingScorerWithMax], threshold: f32) -> Option<(usize, usize, u32)> {
//     let mut max_score = 0.0;
//     let mut before_pivot_len = 0;
//     let mut pivot_doc = u32::MAX;
//     while before_pivot_len < scorers.len() {
//         let term_scorer = &scorers[before_pivot_len];
//         max_score += term_scorer.max_score;
//         if max_score > threshold {
//             pivot_doc = term_scorer.postings.doc_id();
//             break;
//         }
//         before_pivot_len += 1;
//     }
//     if pivot_doc == u32::MAX {
//         return None;
//     }
//     let mut pivot_len = before_pivot_len + 1;
//     pivot_len += scorers[pivot_len..]
//         .iter()
//         .take_while(|term_scorer| term_scorer.postings.doc_id() == pivot_doc)
//         .count();
//     Some((before_pivot_len, pivot_len, pivot_doc))
// }

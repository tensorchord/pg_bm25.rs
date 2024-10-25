use std::num::NonZero;

use pgrx::{prelude::PgHeapTuple, FromDatum};

use crate::{
    datatype::{Bm25VectorBorrowed, Bm25VectorOutput},
    field_norm::{id_to_fieldnorm, FieldNormReader},
    guc::BM25_LIMIT,
    page::{page_read, MetaPageData, METAPAGE_BLKNO},
    payload::PayloadReader,
    postings::{InvertedReader, PostingReader, TERMINATED_DOC},
    utils::topk_computer::TopKComputer,
    weight::{idf, Bm25Weight},
};

enum Scanner {
    Initial,
    Waiting {
        query_index: pgrx::PgRelation,
        query_vector: Bm25VectorOutput,
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
    assert!(
        direction == pgrx::pg_sys::ScanDirection::ForwardScanDirection,
        "only support forward scan"
    );
    let scanner = unsafe { (*scan).opaque.cast::<Scanner>().as_mut().unwrap() };
    let results = match scanner {
        Scanner::Initial => return false,
        Scanner::Waiting {
            query_index,
            query_vector,
        } => {
            let results = scan_main(query_index.as_ptr(), query_vector.as_ref());
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

// return top-k results
unsafe fn scan_main(index: pgrx::pg_sys::Relation, query_vector: Bm25VectorBorrowed) -> Vec<u64> {
    let meta = {
        let page = page_read(index, METAPAGE_BLKNO);
        unsafe { (page.data().as_ptr() as *const MetaPageData).read() }
    };

    let inverted_reader = InvertedReader::new(index, &meta);

    let posting_readers = query_vector
        .indexes()
        .iter()
        .zip(query_vector.values())
        .filter_map(|(&term_id, &term_tf)| {
            Some((inverted_reader.get_posting_reader(term_id)?, term_tf))
        })
        .collect::<Vec<_>>();

    let fieldnorm_reader = FieldNormReader::new(index, meta.field_norms_blkno);
    let mut results;
    if posting_readers.len() == 1 {
        let idf = idf(meta.doc_cnt, posting_readers[0].0.doc_count());
        let bm25_weight = Bm25Weight::new(posting_readers[0].1, idf, meta.avg_dl);
        let scorer = PostingScorer {
            postings: posting_readers.into_iter().next().unwrap().0,
            weight: bm25_weight,
        };
        results = block_wand_single(scorer, &fieldnorm_reader);
    } else {
        let scorers = posting_readers
            .into_iter()
            .map(|(r, tf)| {
                let idf = idf(meta.doc_cnt, r.doc_count());
                let bm25_weight = Bm25Weight::new(tf, idf, meta.avg_dl);
                PostingScorerWithMax {
                    postings: r,
                    weight: bm25_weight,
                    max_score: bm25_weight.max_score(),
                }
            })
            .collect::<Vec<_>>();
        results = block_wand(scorers, &fieldnorm_reader);
    }

    let payload_reader = PayloadReader::new(index, meta.payload_blkno);
    results
        .to_sorted_slice()
        .iter()
        .map(|(_, doc_id)| payload_reader.read(*doc_id))
        .collect()
}

struct PostingScorer {
    postings: PostingReader,
    weight: Bm25Weight,
}

fn block_wand_single(
    mut scorer: PostingScorer,
    fieldnorm_reader: &FieldNormReader,
) -> TopKComputer {
    let mut results = TopKComputer::new(BM25_LIMIT.get() as usize);
    'outer: loop {
        while scorer.postings.block_max_score(&scorer.weight) <= results.threshold() {
            if !scorer.postings.advance_block() {
                break 'outer;
            }
        }
        scorer.postings.decode_block();
        loop {
            let doc_id = scorer.postings.doc_id();
            let tf = scorer.postings.term_freq();
            let fieldnorm_id = fieldnorm_reader.read(doc_id);
            let fieldnorm = id_to_fieldnorm(fieldnorm_id);
            let score = scorer.weight.score(fieldnorm, tf);
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

#[derive(Debug)]
struct PostingScorerWithMax {
    postings: PostingReader,
    weight: Bm25Weight,
    max_score: f32,
}

fn block_wand(
    mut scorers: Vec<PostingScorerWithMax>,
    fieldnorm_reader: &FieldNormReader,
) -> TopKComputer {
    let mut results = TopKComputer::new(BM25_LIMIT.get() as usize);

    for s in &mut scorers {
        s.postings.decode_block();
    }
    scorers.sort_by_key(|s| s.postings.doc_id());

    while let Some((before_pivot_len, pivot_len, pivot_doc)) =
        find_pivot_doc(&scorers, results.threshold())
    {
        let block_max_score_upperbound: f32 = scorers[..pivot_len]
            .iter_mut()
            .map(|scorer| {
                scorer.postings.shallow_seek(pivot_doc);
                scorer.postings.block_max_score(&scorer.weight)
            })
            .sum();

        if block_max_score_upperbound <= results.threshold() {
            block_max_was_too_low_advance_one_scorer(&mut scorers, pivot_len);
            continue;
        }

        if !align_scorers(&mut scorers, pivot_doc, before_pivot_len) {
            continue;
        }

        let len = id_to_fieldnorm(fieldnorm_reader.read(pivot_doc));
        let score = scorers[..pivot_len]
            .iter()
            .map(|scorer| scorer.weight.score(len, scorer.postings.term_freq()))
            .sum();

        results.push(score, pivot_doc);

        advance_all_scorers_on_pivot(&mut scorers, pivot_len);
    }

    results
}

fn find_pivot_doc(scorers: &[PostingScorerWithMax], threshold: f32) -> Option<(usize, usize, u32)> {
    let mut max_score = 0.0;
    let mut before_pivot_len = 0;
    let mut pivot_doc = u32::MAX;
    while before_pivot_len < scorers.len() {
        let scorer = &scorers[before_pivot_len];
        max_score += scorer.max_score;
        if max_score > threshold {
            pivot_doc = scorer.postings.doc_id();
            break;
        }
        before_pivot_len += 1;
    }
    if pivot_doc == u32::MAX {
        return None;
    }

    let mut pivot_len = before_pivot_len + 1;
    pivot_len += scorers[pivot_len..]
        .iter()
        .take_while(|term_scorer| term_scorer.postings.doc_id() == pivot_doc)
        .count();
    Some((before_pivot_len, pivot_len, pivot_doc))
}

fn block_max_was_too_low_advance_one_scorer(
    scorers: &mut [PostingScorerWithMax],
    pivot_len: usize,
) {
    let mut scorer_to_seek = pivot_len - 1;
    let mut global_max_score = scorers[scorer_to_seek].max_score;
    let mut doc_to_seek_after = scorers[scorer_to_seek].postings.last_doc_in_block();

    for scorer_ord in (0..pivot_len - 1).rev() {
        let scorer = &scorers[scorer_ord];
        if scorer.postings.last_doc_in_block() <= doc_to_seek_after {
            doc_to_seek_after = scorer.postings.last_doc_in_block();
        }
        if scorers[scorer_ord].max_score > global_max_score {
            global_max_score = scorers[scorer_ord].max_score;
            scorer_to_seek = scorer_ord;
        }
    }
    doc_to_seek_after = doc_to_seek_after.saturating_add(1);

    for scorer in &mut scorers[pivot_len..] {
        if scorer.postings.doc_id() <= doc_to_seek_after {
            doc_to_seek_after = scorer.postings.doc_id();
        }
    }
    scorers[scorer_to_seek].postings.seek(doc_to_seek_after);

    restore_ordering(scorers, scorer_to_seek);
}

fn restore_ordering(term_scorers: &mut [PostingScorerWithMax], ord: usize) {
    let doc = term_scorers[ord].postings.doc_id();
    for i in ord + 1..term_scorers.len() {
        if term_scorers[i].postings.doc_id() >= doc {
            break;
        }
        term_scorers.swap(i, i - 1);
    }
}

fn align_scorers(
    term_scorers: &mut Vec<PostingScorerWithMax>,
    pivot_doc: u32,
    before_pivot_len: usize,
) -> bool {
    for i in (0..before_pivot_len).rev() {
        let new_doc = term_scorers[i].postings.seek(pivot_doc);
        if new_doc != pivot_doc {
            if new_doc == TERMINATED_DOC {
                term_scorers.swap_remove(i);
            }
            restore_ordering(term_scorers, i);
            return false;
        }
    }
    true
}

fn advance_all_scorers_on_pivot(term_scorers: &mut Vec<PostingScorerWithMax>, pivot_len: usize) {
    for scorer in &mut term_scorers[..pivot_len] {
        scorer.postings.advance();
    }
    term_scorers.retain(|scorer| !scorer.postings.completed());
    term_scorers.sort_unstable_by_key(|scorer| scorer.postings.doc_id());
}

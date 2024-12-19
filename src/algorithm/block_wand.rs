use crate::{
    segment::{
        delete::DeleteBitmapReader,
        field_norm::{id_to_fieldnorm, FieldNormRead, FieldNormReader},
        posting::{PostingCursor, TERMINATED_DOC},
    },
    utils::topk_computer::TopKComputer,
    weight::Bm25Weight,
};

pub struct SealedScorer {
    pub posting: PostingCursor,
    pub weight: Bm25Weight,
    pub max_score: f32,
}

pub fn block_wand_single(
    mut scorer: SealedScorer,
    fieldnorm_reader: &FieldNormReader,
    delete_bitmap_reader: &DeleteBitmapReader,
    computer: &mut TopKComputer,
) {
    'outer: loop {
        while scorer.posting.block_max_score(&scorer.weight) <= computer.threshold() {
            if !scorer.posting.next_block() {
                break 'outer;
            }
        }
        scorer.posting.decode_block();
        loop {
            let doc_id = scorer.posting.docid();
            if !delete_bitmap_reader.is_delete(doc_id) {
                let tf = scorer.posting.freq();
                let fieldnorm_id = fieldnorm_reader.read(doc_id);
                let fieldnorm = id_to_fieldnorm(fieldnorm_id);
                let score = scorer.weight.score(fieldnorm, tf);
                computer.push(score, scorer.posting.docid());
            }
            if !scorer.posting.next_doc() {
                break;
            }
        }
        if !scorer.posting.next_block() {
            break;
        }
    }
}

pub fn block_wand(
    mut scorers: Vec<SealedScorer>,
    fieldnorm_reader: &FieldNormReader,
    delete_bitmap_reader: &DeleteBitmapReader,
    computer: &mut TopKComputer,
) {
    for s in &mut scorers {
        s.posting.decode_block();
    }
    scorers.sort_by_key(|s| s.posting.docid());

    while let Some((before_pivot_len, pivot_len, pivot_doc)) =
        find_pivot_doc(&scorers, computer.threshold())
    {
        let block_max_score_upperbound: f32 = scorers[..pivot_len]
            .iter_mut()
            .map(|scorer| {
                scorer.posting.shallow_seek(pivot_doc);
                scorer.posting.block_max_score(&scorer.weight)
            })
            .sum();

        if block_max_score_upperbound <= computer.threshold() {
            block_max_was_too_low_advance_one_scorer(&mut scorers, pivot_len);
            continue;
        }

        if !align_scorers(&mut scorers, pivot_doc, before_pivot_len) {
            continue;
        }

        if !delete_bitmap_reader.is_delete(pivot_doc) {
            let len = id_to_fieldnorm(fieldnorm_reader.read(pivot_doc));
            let score = scorers[..pivot_len]
                .iter()
                .map(|scorer| scorer.weight.score(len, scorer.posting.freq()))
                .sum();
            computer.push(score, pivot_doc);
        }

        advance_all_scorers_on_pivot(&mut scorers, pivot_len);
    }
}

fn find_pivot_doc(scorers: &[SealedScorer], threshold: f32) -> Option<(usize, usize, u32)> {
    let mut max_score = 0.0;
    let mut before_pivot_len = 0;
    let mut pivot_doc = u32::MAX;
    while before_pivot_len < scorers.len() {
        let scorer = &scorers[before_pivot_len];
        max_score += scorer.max_score;
        if max_score > threshold {
            pivot_doc = scorer.posting.docid();
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
        .take_while(|term_scorer| term_scorer.posting.docid() == pivot_doc)
        .count();
    Some((before_pivot_len, pivot_len, pivot_doc))
}

fn block_max_was_too_low_advance_one_scorer(scorers: &mut [SealedScorer], pivot_len: usize) {
    let mut scorer_to_seek = pivot_len - 1;
    let mut global_max_score = scorers[scorer_to_seek].max_score;
    let mut doc_to_seek_after = scorers[scorer_to_seek].posting.last_doc_in_block();

    for scorer_ord in (0..pivot_len - 1).rev() {
        let scorer = &scorers[scorer_ord];
        if scorer.posting.last_doc_in_block() <= doc_to_seek_after {
            doc_to_seek_after = scorer.posting.last_doc_in_block();
        }
        if scorers[scorer_ord].max_score > global_max_score {
            global_max_score = scorers[scorer_ord].max_score;
            scorer_to_seek = scorer_ord;
        }
    }
    doc_to_seek_after = doc_to_seek_after.saturating_add(1);

    for scorer in &mut scorers[pivot_len..] {
        if scorer.posting.docid() <= doc_to_seek_after {
            doc_to_seek_after = scorer.posting.docid();
        }
    }
    scorers[scorer_to_seek].posting.seek(doc_to_seek_after);

    restore_ordering(scorers, scorer_to_seek);
}

fn restore_ordering(term_scorers: &mut [SealedScorer], ord: usize) {
    let doc = term_scorers[ord].posting.docid();
    for i in ord + 1..term_scorers.len() {
        if term_scorers[i].posting.docid() >= doc {
            break;
        }
        term_scorers.swap(i, i - 1);
    }
}

fn align_scorers(
    term_scorers: &mut Vec<SealedScorer>,
    pivot_doc: u32,
    before_pivot_len: usize,
) -> bool {
    for i in (0..before_pivot_len).rev() {
        let new_doc = term_scorers[i].posting.seek(pivot_doc);
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

fn advance_all_scorers_on_pivot(term_scorers: &mut Vec<SealedScorer>, pivot_len: usize) {
    for scorer in &mut term_scorers[..pivot_len] {
        scorer.posting.next_with_auto_decode();
    }
    term_scorers.retain(|scorer| !scorer.posting.completed());
    term_scorers.sort_unstable_by_key(|scorer| scorer.posting.docid());
}

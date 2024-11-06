use crate::{
    datatype::Bm25VectorBorrowed,
    field_norm::{fieldnorm_to_id, id_to_fieldnorm},
    term_info::TermInfoReader,
};

const K1: f32 = 1.2;
const B: f32 = 0.75;

#[derive(Clone, Copy, Debug)]
pub struct Bm25Weight {
    weight: f32, // idf * (1 + K1) * term_count
    avgdl: f32,
}

impl Bm25Weight {
    pub fn new(count: u32, idf: f32, avgdl: f32) -> Self {
        let weight = count as f32 * idf * (1.0 + K1);
        Self { weight, avgdl }
    }

    #[inline]
    pub fn score(&self, len: u32, tf: u32) -> f32 {
        let len = len as f32;
        let tf = tf as f32;
        self.weight * tf / (tf + K1 * (1.0 - B + B * len / self.avgdl))
    }

    pub fn max_score(&self) -> f32 {
        self.score(2_013_265_944, 2_013_265_944)
    }
}

// ln ( (N + 1) / (n(q) + 0.5) )
#[inline]
pub fn idf(doc_cnt: u32, doc_freq: u32) -> f32 {
    (((doc_cnt + 1) as f32) / (doc_freq as f32 + 0.5)).ln()
}

pub fn bm25_score_batch(
    doc_cnt: u32,
    avgdl: f32,
    term_info_reader: &TermInfoReader,
    target_vector: Bm25VectorBorrowed,
    query_vector: Bm25VectorBorrowed,
) -> f32 {
    use std::cmp::Ordering;
    let doc_len = id_to_fieldnorm(fieldnorm_to_id(target_vector.doc_len()));
    let precompute = K1 * (1.0 - B + B * doc_len as f32 / avgdl);
    let (li, lv) = (target_vector.indexes(), target_vector.values());
    let (mut lp, ln) = (0, target_vector.len() as usize);
    let (ri, rv) = (query_vector.indexes(), query_vector.values());
    let (mut rp, rn) = (0, query_vector.len() as usize);
    let mut scores: f32 = 0.0;
    while lp < ln && rp < rn {
        match Ord::cmp(&li[lp], &ri[rp]) {
            Ordering::Equal => {
                let idf = idf(doc_cnt, term_info_reader.read(li[lp]));
                let tf = lv[lp] as f32;
                let res = rv[rp] as f32 * idf * (K1 + 1.0) * tf / (tf + precompute);
                scores += res;
                lp += 1;
                rp += 1;
            }
            Ordering::Less => {
                lp += 1;
            }
            Ordering::Greater => {
                rp += 1;
            }
        }
    }
    scores
}

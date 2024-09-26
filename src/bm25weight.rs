const K1: f32 = 1.2;
const B: f32 = 0.75;

#[derive(Clone, Copy)]
pub struct Bm25Weight {
    weight: f32, // idf * (1 + K1)
    avgdl: f32,
}

impl Bm25Weight {
    pub fn new(idf: f32, avgdl: f32) -> Self {
        let weight = idf * (1.0 + K1);
        Self { weight, avgdl }
    }

    #[inline]
    pub fn score(&self, len: u32, tf: u32) -> f32 {
        let len = len as f32;
        let tf = tf as f32;
        self.weight * tf / (tf + K1 * (1.0 - B + B * len / self.avgdl))
    }

    #[allow(dead_code)]
    pub fn max_score(&self) -> f32 {
        self.score(2_013_265_944, 2_013_265_944)
    }
}

// ln { (N + 1) / (n(q) + 0.5) }
pub fn idf(doc_cnt: u32, doc_freq: u32) -> f32 {
    assert!(doc_cnt >= doc_freq);
    (((doc_cnt + 1) as f32) / (doc_freq as f32 + 0.5)).ln()
}

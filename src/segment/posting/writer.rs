use super::InvertedWrite;
use crate::{datatype::Bm25VectorBorrowed, token::vocab_len, utils::vint};

// inverted lists in memory
pub struct InvertedWriter {
    term_index: Vec<TFRecorder>,
}

impl InvertedWriter {
    pub fn new() -> Self {
        Self {
            term_index: (0..vocab_len()).map(|_| TFRecorder::new()).collect(),
        }
    }

    pub fn insert(&mut self, doc_id: u32, vector: Bm25VectorBorrowed) {
        for (&term_id, &tf) in vector.indexes().iter().zip(vector.values()) {
            let tf_recorder = &mut self.term_index[term_id as usize];
            if tf_recorder.current_doc() != doc_id {
                tf_recorder.try_close_doc();
                tf_recorder.new_doc(doc_id);
            }
            tf_recorder.record(tf);
        }
    }

    pub fn finalize(&mut self) {
        for recorder in &mut self.term_index {
            recorder.try_close_doc();
        }
    }

    pub fn serialize<I: InvertedWrite>(&self, s: &mut I) {
        for recorder in &self.term_index {
            s.write(recorder);
        }
    }

    pub fn term_stat(&self) -> impl Iterator<Item = u32> + '_ {
        self.term_index.iter().map(|recorder| recorder.doc_cnt)
    }
}

// Store (doc_id, tf) tuples, doc_id is delta encoded
pub struct TFRecorder {
    buffer: Vec<u8>,
    current_doc: u32,
    current_tf: u32,
    doc_cnt: u32,
}

impl TFRecorder {
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            current_doc: u32::MAX,
            current_tf: 0,
            doc_cnt: 0,
        }
    }

    fn current_doc(&self) -> u32 {
        self.current_doc
    }

    fn new_doc(&mut self, doc_id: u32) {
        let delta = doc_id.wrapping_sub(self.current_doc);
        self.doc_cnt += 1;
        self.current_doc = doc_id;
        vint::encode_vint32(delta, &mut self.buffer).unwrap();
    }

    fn record(&mut self, count: u32) {
        self.current_tf += count;
    }

    fn try_close_doc(&mut self) {
        if self.current_tf == 0 {
            return;
        }
        vint::encode_vint32(self.current_tf, &mut self.buffer).unwrap();
        self.current_tf = 0;
    }

    pub fn iter(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        let mut doc_id = u32::MAX;
        let mut buffer = self.buffer.as_slice();
        std::iter::from_fn(move || {
            if buffer.is_empty() {
                return None;
            }
            let delta_doc_id = vint::decode_vint32(&mut buffer);
            let tf = vint::decode_vint32(&mut buffer);
            doc_id = doc_id.wrapping_add(delta_doc_id);
            Some((doc_id, tf))
        })
    }

    pub fn doc_cnt(&self) -> u32 {
        self.doc_cnt
    }
}

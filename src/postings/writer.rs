use super::InvertedSerializer;
use crate::{datatype::Bm25VectorBorrowed, token::VOCAB_LEN, utils::vint};

// postings in ram
pub struct PostingsWriter {
    term_index: Vec<TFRecorder>,
}

impl PostingsWriter {
    pub fn new() -> Self {
        Self {
            term_index: (0..*VOCAB_LEN).map(|_| TFRecorder::new()).collect(),
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

    pub fn serialize(&self, s: &mut InvertedSerializer) {
        for recorder in &self.term_index {
            s.new_term(recorder.total_docs);
            for (doc_id, tf) in recorder.iter() {
                s.write_doc(doc_id, tf);
            }
            s.close_term();
        }
    }
}

// Store (doc_id, tf) tuples, doc_id is delta encoded
struct TFRecorder {
    buffer: Vec<u8>,
    current_doc: u32,
    current_tf: u32,
    total_docs: u32,
}

impl TFRecorder {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            current_doc: u32::MAX,
            current_tf: 0,
            total_docs: 0,
        }
    }

    fn current_doc(&self) -> u32 {
        self.current_doc
    }

    fn new_doc(&mut self, doc_id: u32) {
        let delta = doc_id.wrapping_sub(self.current_doc);
        self.total_docs += 1;
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

    fn iter(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
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
}

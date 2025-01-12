use std::collections::BTreeMap;

use super::InvertedSerialize;
use crate::{datatype::Bm25VectorBorrowed, utils::vint};

// inverted lists in memory
pub struct InvertedWriter {
    term_index: BTreeMap<u32, TFRecorder>,
}

impl InvertedWriter {
    pub fn new() -> Self {
        Self {
            term_index: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, doc_id: u32, vector: Bm25VectorBorrowed) {
        for (&term_id, &tf) in vector.indexes().iter().zip(vector.values()) {
            let tf_recorder = self
                .term_index
                .entry(term_id)
                .or_insert_with(TFRecorder::new);
            if tf_recorder.current_doc() != doc_id {
                tf_recorder.try_close_doc();
                tf_recorder.new_doc(doc_id);
            }
            tf_recorder.record(tf);
        }
    }

    pub fn finalize(&mut self) {
        for recorder in self.term_index.values_mut() {
            recorder.try_close_doc();
        }
    }

    pub fn serialize<I: InvertedSerialize>(&self, s: &mut I) {
        let mut last_term_id = 0;
        for (&term_id, recorder) in &self.term_index {
            for _ in last_term_id..term_id {
                s.new_term(0);
                s.close_term();
            }

            s.new_term(recorder.total_docs);
            for (doc_id, tf) in recorder.iter() {
                s.write_doc(doc_id, tf);
            }
            s.close_term();
            last_term_id = term_id + 1;
        }
    }

    pub fn term_stat(&self) -> impl Iterator<Item = u32> + '_ {
        let mut iter = self.term_index.iter().peekable();
        let mut last_term_id = 0;
        std::iter::from_fn(move || {
            let (&term_id, recorder) = iter.peek()?;
            if last_term_id < term_id {
                last_term_id += 1;
                return Some(0);
            }
            let total_docs = recorder.total_docs;
            iter.next();
            last_term_id += 1;
            Some(total_docs)
        })
    }

    pub fn term_id_cnt(&self) -> u32 {
        self.term_index
            .last_key_value()
            .map(|(k, _)| *k + 1)
            .unwrap_or(0)
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
    fn new() -> Self {
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

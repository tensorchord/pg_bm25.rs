use std::collections::BTreeMap;

use super::InvertedSerializer;
use crate::utils::vint;

// postings in ram
pub struct PostingsWriter {
    term_index: BTreeMap<String, TFRecorder>, // TODO: optimize with arena hashmap
}

impl PostingsWriter {
    pub fn new() -> Self {
        Self {
            term_index: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, id: u32, tokens: &[String]) {
        for token in tokens {
            let tf_recorder = self.term_index.entry(token.clone()).or_insert({
                let mut recorder = TFRecorder::new();
                recorder.new_doc(id);
                recorder
            });
            if tf_recorder.current_doc() != id {
                tf_recorder.close_doc();
                tf_recorder.new_doc(id);
            }
            tf_recorder.record();
        }
    }

    pub fn finalize(&mut self) {
        for recorder in self.term_index.values_mut() {
            if !recorder.closed() {
                recorder.close_doc();
            }
        }
    }

    pub fn serialize(&self, s: &mut InvertedSerializer) -> anyhow::Result<()> {
        for (term, recorder) in &self.term_index {
            s.new_term(term.as_bytes(), recorder.total_docs)?;
            for (doc_id, tf) in recorder.iter() {
                s.write_doc(doc_id, tf)?;
            }
            s.close_term()?;
        }
        Ok(())
    }
}

// Store (doc_id, tf) tuples, doc_id is delta encoded
struct TFRecorder {
    buffer: Vec<u8>,
    current_doc: u32,
    current_tf: u32,
    total_docs: u32,
    closed: bool,
}

impl TFRecorder {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            current_doc: 0,
            current_tf: 0,
            total_docs: 0,
            closed: true,
        }
    }

    fn current_doc(&self) -> u32 {
        self.current_doc
    }

    fn new_doc(&mut self, doc_id: u32) {
        let delta = doc_id - self.current_doc;
        self.total_docs += 1;
        self.current_doc = doc_id;
        self.closed = false;
        vint::encode_vint32(delta, &mut self.buffer).unwrap();
    }

    fn record(&mut self) {
        self.current_tf += 1;
    }

    fn close_doc(&mut self) {
        vint::encode_vint32(self.current_tf, &mut self.buffer).unwrap();
        self.current_tf = 0;
        self.closed = true;
    }

    fn closed(&self) -> bool {
        self.closed
    }

    fn iter(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        let mut doc_id = 0;
        let mut buffer = self.buffer.as_slice();
        std::iter::from_fn(move || {
            if buffer.is_empty() {
                return None;
            }
            let delta_doc_id = vint::decode_vint32(&mut buffer);
            let tf = vint::decode_vint32(&mut buffer);
            doc_id += delta_doc_id;
            Some((doc_id, tf))
        })
    }
}

use std::io::Write;

use crate::{
    field_norm::FieldNormsWriter,
    page::PageBuilder,
    payload::PayloadWriter,
    postings::{InvertedSerializer, PostingsWriter},
    token,
};

pub struct IndexBuilder {
    doc_cnt: u32,
    doc_term_cnt: u64,
    postings_writer: PostingsWriter,
    field_norms_writer: FieldNormsWriter,
    payload_writer: PayloadWriter,
}

impl IndexBuilder {
    pub fn new() -> Self {
        Self {
            doc_cnt: 0,
            doc_term_cnt: 0,
            postings_writer: PostingsWriter::new(),
            field_norms_writer: FieldNormsWriter::new(),
            payload_writer: PayloadWriter::new(),
        }
    }

    pub fn insert(&mut self, id: u64, document: &[u8]) {
        let tokens = token::BERT_BASE_UNCASED
            .encode(std::str::from_utf8(document).unwrap(), false)
            .expect("failed to tokenize");
        let tokens = tokens.get_tokens();
        self.postings_writer.insert(self.doc_cnt, tokens);
        self.field_norms_writer.insert(self.doc_cnt, tokens);
        self.payload_writer.insert(id);
        self.doc_cnt = self
            .doc_cnt
            .checked_add(1)
            .unwrap_or_else(|| pgrx::error!("bm25 index can only store up to 2^32 - 1 documents"));
        self.doc_term_cnt += tokens.len() as u64;
    }

    pub fn finalize(&mut self) {
        self.postings_writer.finalize();
    }

    pub fn doc_cnt(&self) -> u32 {
        self.doc_cnt
    }

    pub fn avg_dl(&self) -> f32 {
        self.doc_term_cnt as f32 / self.doc_cnt as f32
    }

    pub fn write_payload(&self, pager: &mut PageBuilder) -> anyhow::Result<()> {
        pager
            .write_all(self.payload_writer.data())
            .map_err(Into::into)
    }

    pub fn write_field_norms(&self, pager: &mut PageBuilder) -> anyhow::Result<()> {
        pager
            .write_all(self.field_norms_writer.data())
            .map_err(Into::into)
    }

    // return [term_dict_blk, term_info_blk]
    pub fn write_postings(
        &self,
        index: pgrx::pg_sys::Relation,
    ) -> anyhow::Result<[pgrx::pg_sys::BlockNumber; 2]> {
        let mut inverted_serializer = InvertedSerializer::new(index, self.doc_cnt, self.avg_dl())?;
        self.postings_writer.serialize(&mut inverted_serializer)?;
        inverted_serializer.finalize()
    }
}

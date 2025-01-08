use crate::{datatype::Bm25VectorBorrowed, options::EncodeOption};

use super::{
    field_norm::FieldNormRead,
    posting::{InvertedSerializer, InvertedWriter, PostingCursor, PostingTermInfoReader},
};

#[derive(Debug, Clone, Copy)]
pub struct SealedSegmentData {
    pub term_info_blkno: u32,
}

pub struct SealedSegmentWriter {
    writer: InvertedWriter,
}

impl SealedSegmentWriter {
    pub fn new() -> Self {
        Self {
            writer: InvertedWriter::new(),
        }
    }

    pub fn insert(&mut self, doc_id: u32, vector: Bm25VectorBorrowed) {
        self.writer.insert(doc_id, vector);
    }

    pub fn finalize_insert(&mut self) {
        self.writer.finalize();
    }

    pub fn serialize<R: FieldNormRead>(&self, s: &mut InvertedSerializer<R>) {
        self.writer.serialize(s);
    }
}

pub struct SealedSegmentReader {
    index: pgrx::pg_sys::Relation,
    term_info_reader: PostingTermInfoReader,
}

impl SealedSegmentReader {
    pub fn new(index: pgrx::pg_sys::Relation, sealed_data: SealedSegmentData) -> Self {
        let term_info_reader = PostingTermInfoReader::new(index, sealed_data.term_info_blkno);
        Self {
            index,
            term_info_reader,
        }
    }

    pub fn get_postings(&self, term_id: u32, encode_option: EncodeOption) -> Option<PostingCursor> {
        let term_info = self.term_info_reader.read(term_id);
        if term_info.meta_blkno == pgrx::pg_sys::InvalidBlockNumber {
            return None;
        }
        Some(PostingCursor::new(self.index, term_info, encode_option))
    }
}

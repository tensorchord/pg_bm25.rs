use crate::datatype::Bm25VectorBorrowed;

use super::{
    field_norm::FieldNormRead,
    posting::{InvertedSerializer, InvertedWriter, PostingReader, PostingTermInfoReader},
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

    pub fn get_postings(&self, term_id: u32) -> Option<PostingReader<true>> {
        let term_info = self.term_info_reader.read(term_id);
        if term_info.doc_count == 0 {
            return None;
        }
        Some(PostingReader::new(self.index, term_info))
    }

    pub fn get_postings_docid_only(&self, term_id: u32) -> Option<PostingReader<false>> {
        let term_info = self.term_info_reader.read(term_id);
        if term_info.doc_count == 0 {
            return None;
        }
        Some(PostingReader::new(self.index, term_info))
    }
}

use crate::{datatype::Bm25VectorBorrowed, token::vocab_len};

use super::{
    field_norm::FieldNormRead,
    free_page_lists,
    posting::{InvertedSerializer, PostingReader, PostingTermInfoReader, PostingsWriter},
};

#[derive(Debug, Clone, Copy)]
pub struct SealedSegmentData {
    pub term_info_blkno: u32,
}

pub struct SealedSegmentWriter {
    writer: PostingsWriter,
}

impl SealedSegmentWriter {
    pub fn new() -> Self {
        Self {
            writer: PostingsWriter::new(),
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
        if term_info.postings_blkno == pgrx::pg_sys::InvalidBlockNumber {
            return None;
        }
        Some(PostingReader::new(self.index, term_info))
    }

    pub fn get_postings_docid_only(&self, term_id: u32) -> Option<PostingReader<false>> {
        let term_info = self.term_info_reader.read(term_id);
        if term_info.postings_blkno == pgrx::pg_sys::InvalidBlockNumber {
            return None;
        }
        Some(PostingReader::new(self.index, term_info))
    }
}

pub fn free_sealed_segment(index: pgrx::pg_sys::Relation, sealed_segment: SealedSegmentData) {
    let term_info_reader = PostingTermInfoReader::new(index, sealed_segment.term_info_blkno);

    for i in 0..vocab_len() {
        let term_info = term_info_reader.read(i);
        if term_info.postings_blkno != pgrx::pg_sys::InvalidBlockNumber {
            free_page_lists(index, term_info.postings_blkno);
        }
    }

    free_page_lists(index, sealed_segment.term_info_blkno);
}

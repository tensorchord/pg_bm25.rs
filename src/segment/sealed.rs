use crate::{datatype::Bm25VectorBorrowed, page::VirtualPageWriter};

use super::{
    field_norm::FieldNormRead,
    posting::{
        InvertedSerializer, InvertedWriter, PostingReader, PostingTermInfo, PostingTermInfoReader,
    },
};

#[derive(Debug, Clone, Copy)]
pub struct SealedSegmentData {
    pub term_info_blkno: u32,
    pub term_id_cnt: u32,
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
    term_id_cnt: u32,
}

impl SealedSegmentReader {
    pub fn new(index: pgrx::pg_sys::Relation, sealed_data: SealedSegmentData) -> Self {
        let term_info_reader = PostingTermInfoReader::new(index, sealed_data);
        Self {
            index,
            term_info_reader,
            term_id_cnt: sealed_data.term_id_cnt,
        }
    }

    pub fn get_postings(&self, term_id: u32) -> Option<PostingReader<true>> {
        if term_id >= self.term_id_cnt {
            return None;
        }

        let term_info = self.term_info_reader.read(term_id);
        if term_info.doc_count == 0 {
            return None;
        }
        Some(PostingReader::new(self.index, term_info))
    }

    pub fn get_postings_docid_only(&self, term_id: u32) -> Option<PostingReader<false>> {
        if term_id >= self.term_id_cnt {
            return None;
        }

        let term_info = self.term_info_reader.read(term_id);
        if term_info.doc_count == 0 {
            return None;
        }
        Some(PostingReader::new(self.index, term_info))
    }
}

pub fn extend_sealed_term_id(
    index: pgrx::pg_sys::Relation,
    sealed_data: &mut SealedSegmentData,
    term_id_cnt: u32,
) {
    if sealed_data.term_id_cnt >= term_id_cnt {
        return;
    }
    let mut page_writer = VirtualPageWriter::open(index, sealed_data.term_info_blkno, false);
    for _ in sealed_data.term_id_cnt..term_id_cnt {
        page_writer.write(bytemuck::bytes_of(&PostingTermInfo::empty()));
    }
    sealed_data.term_id_cnt = term_id_cnt;
}

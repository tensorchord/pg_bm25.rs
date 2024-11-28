use crate::datatype::Bm25VectorBorrowed;

use super::{
    field_norm::FieldNormWriter,
    payload::PayloadWriter,
    posting::{InvertedSerializer, InvertedWriter, TERMINATED_DOC},
    sealed::SealedSegmentData,
};

/// Builder for inverted index, used to build the inverted index in memory from empty.
pub struct IndexBuilder {
    doc_cnt: u32,
    doc_term_cnt: u64,
    postings_writer: InvertedWriter,
    field_norm_writer: FieldNormWriter,
    payload_writer: PayloadWriter,
}

impl IndexBuilder {
    pub fn new() -> Self {
        Self {
            doc_cnt: 0,
            doc_term_cnt: 0,
            postings_writer: InvertedWriter::new(),
            field_norm_writer: FieldNormWriter::new(),
            payload_writer: PayloadWriter::new(),
        }
    }

    pub fn insert(&mut self, id: u64, vector: Bm25VectorBorrowed) {
        self.postings_writer.insert(self.doc_cnt, vector);
        self.field_norm_writer.insert(vector.doc_len());
        self.payload_writer.insert(id);
        self.doc_cnt += 1;
        if self.doc_cnt == TERMINATED_DOC {
            pgrx::error!("bm25 index can only store up to 2^32 - 1 documents");
        }
        self.doc_term_cnt += vector.doc_len() as u64;
    }

    pub fn finalize_insert(&mut self) {
        self.postings_writer.finalize();
    }

    // return (payload_blkno, field_norm_blkno, sealed_data)
    pub fn serialize(&self, index: pgrx::pg_sys::Relation) -> (u32, u32, SealedSegmentData) {
        let payload_blkno = self.payload_writer.serialize(index);
        let field_norm_blkno = self.field_norm_writer.serialize(index);

        let mut postings_serializer = InvertedSerializer::new(
            index,
            self.doc_cnt,
            self.doc_term_cnt as f32 / self.doc_cnt as f32,
            self.field_norm_writer.to_memory_reader(),
        );
        self.postings_writer.serialize(&mut postings_serializer);
        let term_info_blkno = postings_serializer.finalize();
        let sealed_data = SealedSegmentData { term_info_blkno };

        (payload_blkno, field_norm_blkno, sealed_data)
    }

    pub fn term_stat(&self) -> impl Iterator<Item = u32> + '_ {
        self.postings_writer.term_stat()
    }

    pub fn doc_cnt(&self) -> u32 {
        self.doc_cnt
    }

    pub fn doc_term_cnt(&self) -> u64 {
        self.doc_term_cnt
    }
}

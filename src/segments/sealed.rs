use crate::{
    datatype::Bm25VectorBorrowed,
    field_norm::FieldNormsWriter,
    page::VirtualPageWriter,
    payload::PayloadWriter,
    postings::{InvertedSerializer, PostingReader, PostingTermInfoReader, PostingsWriter},
};

#[derive(Debug, Clone, Copy)]
pub struct SealedSegmentData {
    pub term_info_blkno: u32,
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

    pub fn get_postings(&self, term_id: u32) -> Option<PostingReader> {
        let term_info = self.term_info_reader.read(term_id);
        if term_info.postings_blkno == pgrx::pg_sys::InvalidBlockNumber {
            return None;
        }
        Some(PostingReader::new(self.index, term_info))
    }
}

pub struct SealedSegmentWriter {
    init_doc_id: u32,
    doc_cnt: u32,
    doc_term_cnt: u64,
    postings_writer: PostingsWriter,
    field_norm_writer: FieldNormsWriter,
    payload_writer: PayloadWriter,
}

impl SealedSegmentWriter {
    pub fn new(init_doc_id: u32) -> Self {
        Self {
            init_doc_id,
            doc_cnt: 0,
            doc_term_cnt: 0,
            postings_writer: PostingsWriter::new(),
            field_norm_writer: FieldNormsWriter::new(),
            payload_writer: PayloadWriter::new(),
        }
    }

    pub fn insert(&mut self, id: u64, vector: Bm25VectorBorrowed) {
        self.postings_writer
            .insert(self.init_doc_id + self.doc_cnt, vector);
        self.field_norm_writer.insert(vector.doc_len());
        self.payload_writer.insert(id);
        self.doc_cnt += 1;
        if self.init_doc_id.checked_add(self.doc_cnt).is_none() {
            pgrx::error!("bm25 index can only store up to 2^32 - 1 documents");
        }
        self.doc_term_cnt += vector.doc_len() as u64;
    }

    pub fn finalize(&mut self) {
        self.postings_writer.finalize();
    }

    pub fn write_payload(&self, pager: &mut VirtualPageWriter) {
        pager.write(self.payload_writer.data())
    }

    pub fn write_field_norm(&self, pager: &mut VirtualPageWriter) {
        pager.write(self.field_norm_writer.data())
    }

    pub fn write_postings(&self, mut serializer: InvertedSerializer) -> SealedSegmentData {
        self.postings_writer.serialize(&mut serializer);
        let term_info_blkno = serializer.finalize();
        SealedSegmentData { term_info_blkno }
    }

    pub fn term_info(&self) -> impl Iterator<Item = u32> + '_ {
        self.postings_writer.term_info()
    }

    pub fn doc_cnt(&self) -> u32 {
        self.doc_cnt
    }

    pub fn doc_term_cnt(&self) -> u64 {
        self.doc_term_cnt
    }
}

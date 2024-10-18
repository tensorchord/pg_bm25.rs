use std::io::Write;

use crate::{
    field_norm::{id_to_fieldnorm, FieldNormReader, MAX_FIELD_NORM},
    page::{page_read, MetaPageData, PageBuilder, PageFlags, METAPAGE_BLKNO},
    token::VOCAB_LEN,
    utils::compress_block::BlockEncoder,
    weight::{idf, Bm25Weight},
};

use super::{SkipBlock, TermInfo, COMPRESSION_BLOCK_SIZE};

pub struct InvertedSerializer {
    postings_serializer: PostingSerializer,
    term_info_serializer: TermInfoSerializer,
    current_term_info: TermInfo,
}

impl InvertedSerializer {
    pub fn new(index: pgrx::pg_sys::Relation, total_doc_cnt: u32, avg_dl: f32) -> Self {
        let postings_serializer = PostingSerializer::new(index, total_doc_cnt, avg_dl);
        let term_info_serializer = TermInfoSerializer::new(index);
        Self {
            postings_serializer,
            term_info_serializer,
            current_term_info: TermInfo::default(),
        }
    }

    pub fn new_term(&mut self, doc_count: u32) {
        self.current_term_info = TermInfo {
            docs: doc_count,
            postings_blkno: pgrx::pg_sys::InvalidBlockNumber,
        };
        if doc_count != 0 {
            self.postings_serializer.new_term(doc_count);
        }
    }

    pub fn write_doc(&mut self, doc_id: u32, freq: u32) {
        self.postings_serializer.write_doc(doc_id, freq);
    }

    pub fn close_term(&mut self) {
        if self.current_term_info.docs != 0 {
            self.current_term_info.postings_blkno = self.postings_serializer.close_term();
        }
        self.term_info_serializer.push(self.current_term_info);
    }

    pub fn finalize(self) -> pgrx::pg_sys::BlockNumber {
        self.term_info_serializer.finalize()
    }
}

struct TermInfoSerializer {
    index: pgrx::pg_sys::Relation,
    term_infos: Vec<TermInfo>,
}

impl TermInfoSerializer {
    pub fn new(index: pgrx::pg_sys::Relation) -> Self {
        Self {
            index,
            term_infos: Vec::with_capacity(*VOCAB_LEN as usize),
        }
    }

    pub fn push(&mut self, term_info: TermInfo) {
        self.term_infos.push(term_info);
    }

    pub fn finalize(self) -> pgrx::pg_sys::BlockNumber {
        let mut pager = PageBuilder::new(self.index, PageFlags::TERM_INFO, true);
        pager
            .write_all(bytemuck::cast_slice(&self.term_infos))
            .unwrap();
        pager.finalize()
    }
}

struct PostingSerializer {
    index: pgrx::pg_sys::Relation,
    encoder: BlockEncoder,
    posting_write: Vec<u8>,
    last_doc_id: u32,
    // block buffer
    doc_ids: [u32; COMPRESSION_BLOCK_SIZE],
    term_freqs: [u32; COMPRESSION_BLOCK_SIZE],
    block_size: usize,
    // block wand helper
    skip_write: Vec<SkipBlock>,
    avg_dl: f32,
    total_doc_cnt: u32,
    bm25_weight: Option<Bm25Weight>,
    filednorm_reader: FieldNormReader,
}

impl PostingSerializer {
    pub fn new(index: pgrx::pg_sys::Relation, total_doc_cnt: u32, avg_dl: f32) -> Self {
        let filednorm_blkno = unsafe {
            let meta_page = page_read(index, METAPAGE_BLKNO);
            (*meta_page.content.as_ptr().cast::<MetaPageData>()).field_norms_blkno
        };

        Self {
            index,
            encoder: BlockEncoder::new(),
            posting_write: Vec::new(),
            last_doc_id: 0,
            doc_ids: [0; COMPRESSION_BLOCK_SIZE],
            term_freqs: [0; COMPRESSION_BLOCK_SIZE],
            block_size: 0,
            skip_write: Vec::new(),
            avg_dl,
            total_doc_cnt,
            bm25_weight: None,
            filednorm_reader: FieldNormReader::new(index, filednorm_blkno),
        }
    }

    pub fn new_term(&mut self, doc_count: u32) {
        let idf = idf(self.total_doc_cnt, doc_count);
        self.bm25_weight = Some(Bm25Weight::new(1, idf, self.avg_dl));
    }

    pub fn write_doc(&mut self, doc_id: u32, freq: u32) {
        self.doc_ids[self.block_size] = doc_id;
        self.term_freqs[self.block_size] = freq;
        self.block_size += 1;
        if self.block_size == COMPRESSION_BLOCK_SIZE {
            self.flush_block();
        }
    }

    pub fn close_term(&mut self) -> pgrx::pg_sys::BlockNumber {
        if self.block_size > 0 {
            if self.block_size == COMPRESSION_BLOCK_SIZE {
                self.flush_block();
            } else {
                self.flush_block_unfull();
            }
        }
        let mut pager = PageBuilder::new(self.index, PageFlags::POSTINGS, true);
        pager
            .write_all(&u32::try_from(self.skip_write.len()).unwrap().to_le_bytes())
            .unwrap();
        pager
            .write_all(bytemuck::cast_slice(self.skip_write.as_slice()))
            .unwrap();
        pager.write_all(&self.posting_write).unwrap();
        let blkno = pager.finalize();
        self.last_doc_id = 0;
        self.bm25_weight = None;
        self.posting_write.clear();
        self.skip_write.clear();
        blkno
    }

    fn flush_block(&mut self) {
        assert!(self.block_size == COMPRESSION_BLOCK_SIZE);

        // doc_id
        let (docid_bits, docid_block) = self
            .encoder
            .compress_block_sorted(&self.doc_ids[..self.block_size], self.last_doc_id);
        self.posting_write.extend_from_slice(docid_block);
        self.last_doc_id = self.doc_ids[self.block_size - 1];

        // term_freq
        for i in 0..self.block_size {
            self.term_freqs[i] -= 1;
        }
        let (tf_bits, term_freq_block) = self
            .encoder
            .compress_block_unsorted(&self.term_freqs[..self.block_size]);
        self.posting_write.extend_from_slice(term_freq_block);

        let (blockwand_tf, blockwand_fieldnorm_id) = self.block_wand();
        let tf_sum = self.doc_ids[..self.block_size].iter().sum();
        self.skip_write.push(SkipBlock {
            last_doc: self.last_doc_id,
            tf_sum,
            docid_bits,
            tf_bits,
            blockwand_tf,
            blockwand_fieldnorm_id,
            reserved: 0,
        });

        self.block_size = 0;
    }

    fn flush_block_unfull(&mut self) {
        assert!(self.block_size > 0);

        // doc_id
        let docid_block = self
            .encoder
            .compress_vint_sorted(&self.doc_ids[..self.block_size], self.last_doc_id);
        self.posting_write.extend_from_slice(docid_block);
        self.last_doc_id = self.doc_ids[self.block_size - 1];

        // term_freq
        for i in 0..self.block_size {
            self.term_freqs[i] -= 1;
        }
        let term_freq_block = self
            .encoder
            .compress_vint_unsorted(&self.term_freqs[..self.block_size]);
        self.posting_write.extend_from_slice(term_freq_block);

        let (blockwand_tf, blockwand_fieldnorm_id) = self.block_wand();
        let tf_sum = self.doc_ids[..self.block_size].iter().sum();
        self.skip_write.push(SkipBlock {
            last_doc: self.last_doc_id,
            tf_sum,
            docid_bits: 0,
            tf_bits: 0,
            blockwand_tf,
            blockwand_fieldnorm_id,
            reserved: 0,
        });

        self.block_size = 0;
    }

    fn block_wand(&self) -> (u32, u8) {
        let mut blockwand_tf = MAX_FIELD_NORM;
        let mut blockwand_fieldnorm_id = u8::MAX;
        let mut blockwand_max = 0.0f32;
        let bm25_weight = self.bm25_weight.as_ref().expect("no bm25 weight");
        for i in 0..self.block_size {
            let doc_id = self.doc_ids[i];
            let tf = self.term_freqs[i];
            let fieldnorm_id = self.filednorm_reader.read(doc_id);
            let len = id_to_fieldnorm(fieldnorm_id);
            let bm25_score = bm25_weight.score(len, tf);
            if bm25_score > blockwand_max {
                blockwand_max = bm25_score;
                blockwand_tf = tf;
                blockwand_fieldnorm_id = fieldnorm_id;
            }
        }
        (blockwand_tf, blockwand_fieldnorm_id)
    }
}

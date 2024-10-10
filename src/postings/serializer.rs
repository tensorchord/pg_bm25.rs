use std::io::Write;

use crate::{
    bm25weight::{idf, Bm25Weight},
    field_norm::{id_to_fieldnorm, FieldNormReader, MAX_FIELD_NORM},
    page::{
        page_get_contents, MetaPageData, PageBuilder, BM25_POSTINGS, BM25_TERM_DICT,
        BM25_TERM_INFO, METAPAGE_BLKNO,
    },
    utils::compress_block::BlockEncoder,
};

use super::{SkipBlock, TermInfo, COMPRESSION_BLOCK_SIZE};

pub struct InvertedSerializer {
    term_dict_serializer: TermDictSerializer,
    postings_serializer: PostingSerializer,
    current_term_info: TermInfo,
}

impl InvertedSerializer {
    pub fn new(
        index: pgrx::pg_sys::Relation,
        total_doc_cnt: u32,
        avg_dl: f32,
    ) -> anyhow::Result<Self> {
        let term_dict_serializer = TermDictSerializer::new(index)?;
        let postings_serializer = PostingSerializer::new(index, total_doc_cnt, avg_dl);
        Ok(Self {
            term_dict_serializer,
            postings_serializer,
            current_term_info: TermInfo::default(),
        })
    }

    pub fn new_term(&mut self, term: &[u8], doc_freq: u32) -> anyhow::Result<()> {
        self.term_dict_serializer.insert_key(term)?;
        self.postings_serializer.new_term(doc_freq);
        self.current_term_info = TermInfo::default();
        Ok(())
    }

    pub fn write_doc(&mut self, doc_id: u32, freq: u32) -> anyhow::Result<()> {
        self.current_term_info.docs += 1;
        self.postings_serializer.write_doc(doc_id, freq)?;
        Ok(())
    }

    pub fn close_term(&mut self) -> anyhow::Result<()> {
        let blkno = self.postings_serializer.close_term()?;
        self.current_term_info.postings_blkno = blkno;
        self.term_dict_serializer
            .insert_value(self.current_term_info);
        Ok(())
    }

    // return [term_dict_blk, term_info_blk]
    pub fn finalize(self) -> anyhow::Result<[pgrx::pg_sys::BlockNumber; 2]> {
        self.term_dict_serializer.finalize()
    }
}

struct TermDictSerializer {
    index: pgrx::pg_sys::Relation,
    term_ord: u64,
    fst_builder: fst::MapBuilder<PageBuilder>,
    term_infos: Vec<TermInfo>, // TODO: use bitpacking
}

impl TermDictSerializer {
    pub fn new(index: pgrx::pg_sys::Relation) -> anyhow::Result<Self> {
        let pager = PageBuilder::new(index, BM25_TERM_DICT, false);
        Ok(Self {
            index,
            term_ord: 0,
            fst_builder: fst::MapBuilder::new(pager)?,
            term_infos: Vec::new(),
        })
    }

    pub fn insert_key(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.fst_builder
            .insert(key, self.term_ord)
            .map_err(|e| anyhow::anyhow!("failed to insert key: {:?}", e))?;
        self.term_ord += 1;
        Ok(())
    }

    pub fn insert_value(&mut self, value: TermInfo) {
        self.term_infos.push(value);
    }

    // return [term_dict_blk, term_info_blk]
    pub fn finalize(self) -> anyhow::Result<[pgrx::pg_sys::BlockNumber; 2]> {
        let term_dict_pager = self.fst_builder.into_inner()?;
        let term_dict_blk = term_dict_pager.finalize();

        let mut term_info_pager = PageBuilder::new(self.index, BM25_TERM_INFO, true);
        term_info_pager.write_all(bytemuck::cast_slice(&self.term_infos))?;
        let term_info_blk = term_info_pager.finalize();

        Ok([term_dict_blk, term_info_blk])
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
            let meta_buffer = pgrx::pg_sys::ReadBuffer(index, METAPAGE_BLKNO);
            pgrx::pg_sys::LockBuffer(meta_buffer, pgrx::pg_sys::BUFFER_LOCK_SHARE as _);
            let meta_page = pgrx::pg_sys::BufferGetPage(meta_buffer);
            let meta_data: *mut MetaPageData = page_get_contents(meta_page);
            let blkno = (*meta_data).field_norms_blkno;
            pgrx::pg_sys::UnlockReleaseBuffer(meta_buffer);
            blkno
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

    pub fn new_term(&mut self, doc_freq: u32) {
        let idf = idf(self.total_doc_cnt, doc_freq);
        self.bm25_weight = Some(Bm25Weight::new(idf, self.avg_dl));
    }

    pub fn write_doc(&mut self, doc_id: u32, freq: u32) -> anyhow::Result<()> {
        self.doc_ids[self.block_size] = doc_id;
        self.term_freqs[self.block_size] = freq;
        self.block_size += 1;
        if self.block_size == COMPRESSION_BLOCK_SIZE {
            self.flush_block()?;
        }
        Ok(())
    }

    pub fn close_term(&mut self) -> anyhow::Result<pgrx::pg_sys::BlockNumber> {
        if self.block_size > 0 {
            if self.block_size == COMPRESSION_BLOCK_SIZE {
                self.flush_block()?;
            } else {
                self.flush_block_unfull()?;
            }
        }
        let mut pager = PageBuilder::new(self.index, BM25_POSTINGS, false);
        pager.write_all(&u32::try_from(self.skip_write.len()).unwrap().to_le_bytes())?;
        pager.write_all(bytemuck::cast_slice(self.skip_write.as_slice()))?;
        pager.write_all(&self.posting_write)?;
        let blkno = pager.finalize();
        self.last_doc_id = 0;
        self.bm25_weight = None;
        self.posting_write.clear();
        self.skip_write.clear();
        Ok(blkno)
    }

    fn flush_block(&mut self) -> anyhow::Result<()> {
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
        });

        self.block_size = 0;

        Ok(())
    }

    fn flush_block_unfull(&mut self) -> anyhow::Result<()> {
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
        });

        self.block_size = 0;

        Ok(())
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

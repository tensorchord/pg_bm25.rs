use std::num::NonZeroU32;

use crate::{
    algorithm::{BlockEncode, BlockEncodeTrait, BlockPartition, BlockPartitionTrait},
    options::{EncodeOption, PartitionOption},
    page::{page_alloc, PageFlags, PageWriter, VirtualPageWriter},
    segment::{
        field_norm::{id_to_fieldnorm, FieldNormRead},
        posting::SkipBlockFlags,
    },
    token::vocab_len,
    weight::{idf, Bm25Weight},
};

use super::{writer::TFRecorder, PostingTermInfo, PostingTermMetaData, SkipBlock};

pub trait InvertedWrite {
    fn write(&mut self, recorder: &TFRecorder);
}

pub struct InvertedSerializer<R: FieldNormRead> {
    index: pgrx::pg_sys::Relation,
    postings_serializer: PostingSerializer,
    term_info_serializer: PostingTermInfoSerializer,
    block_parttion: BlockPartition,
    // block wand helper
    avgdl: f32,
    corpus_doc_cnt: u32,
    fieldnorm_reader: R,
}

impl<R: FieldNormRead> InvertedSerializer<R> {
    pub fn new(
        index: pgrx::pg_sys::Relation,
        corpus_doc_cnt: u32,
        avgdl: f32,
        fieldnorm_reader: R,
        partition_option: PartitionOption,
        encode_option: EncodeOption,
    ) -> Self {
        let postings_serializer = PostingSerializer::new(index, encode_option);
        let term_info_serializer = PostingTermInfoSerializer::new(index);
        Self {
            index,
            postings_serializer,
            term_info_serializer,
            block_parttion: BlockPartition::new(partition_option),
            avgdl,
            corpus_doc_cnt,
            fieldnorm_reader,
        }
    }

    /// return term_info_blkno
    pub fn finalize(self) -> pgrx::pg_sys::BlockNumber {
        self.term_info_serializer.finalize()
    }
}

impl<R: FieldNormRead> InvertedWrite for InvertedSerializer<R> {
    fn write(&mut self, recorder: &TFRecorder) {
        let doc_cnt = recorder.doc_cnt();
        if doc_cnt == 0 {
            self.term_info_serializer.push(PostingTermInfo::empty());
            return;
        }

        let idf = idf(self.corpus_doc_cnt, doc_cnt);
        let bm25_weight = Bm25Weight::new(1, idf, self.avgdl);
        for (doc_id, tf) in recorder.iter() {
            let len = id_to_fieldnorm(self.fieldnorm_reader.read(doc_id));
            self.block_parttion.add_doc(bm25_weight.score(len, tf));
        }
        self.block_parttion.make_partitions();
        let partitions = self.block_parttion.partitions();
        let max_doc = self.block_parttion.max_doc();
        let mut block_count = 0;
        let mut blockwand_tf = 0;
        let mut blockwand_fieldnorm_id = 0;

        self.postings_serializer.new_term();
        for (i, (doc_id, freq)) in recorder.iter().enumerate() {
            self.postings_serializer.write_doc(doc_id, freq);
            if Some(i as u32) == partitions.get(block_count).copied() {
                self.postings_serializer
                    .flush_block(blockwand_tf, blockwand_fieldnorm_id);
                block_count += 1;
            }
            if Some(i as u32) == max_doc.get(block_count).copied() {
                blockwand_tf = freq;
                blockwand_fieldnorm_id = self.fieldnorm_reader.read(doc_id);
            }
        }
        assert!(block_count == partitions.len());
        self.block_parttion.reset();

        let mut term_meta_guard = page_alloc(self.index, PageFlags::TERM_META, true);
        let term_meta_page = &mut *term_meta_guard;
        term_meta_page.header.pd_lower += std::mem::size_of::<PostingTermMetaData>() as u16;
        let term_meta: &mut PostingTermMetaData = term_meta_page.as_mut();

        let (unflushed_docids, unflushed_term_freqs) = self.postings_serializer.unflushed_data();
        let unfulled_doc_cnt = unflushed_docids.len();
        assert!(unfulled_doc_cnt < 128);
        term_meta.unfulled_docid[..unfulled_doc_cnt].copy_from_slice(unflushed_docids);
        term_meta.unfulled_freq[..unfulled_doc_cnt].copy_from_slice(unflushed_term_freqs);
        term_meta.unfulled_doc_cnt = unfulled_doc_cnt as u32;
        if unfulled_doc_cnt != 0 {
            block_count += 1;
        }

        term_meta.last_full_block_last_docid =
            NonZeroU32::new(self.postings_serializer.prev_block_last_doc_id());
        let (skip_info_blkno, skip_info_last_blkno, block_data_blkno) = self
            .postings_serializer
            .close_term(&bm25_weight, &self.fieldnorm_reader);
        term_meta.block_count = block_count.try_into().unwrap();
        term_meta.skip_info_blkno = skip_info_blkno;
        term_meta.skip_info_last_blkno = skip_info_last_blkno;
        term_meta.block_data_blkno = block_data_blkno;

        self.term_info_serializer.push(PostingTermInfo {
            meta_blkno: term_meta_guard.blkno(),
        });
    }
}

struct PostingTermInfoSerializer {
    index: pgrx::pg_sys::Relation,
    term_infos: Vec<PostingTermInfo>,
}

impl PostingTermInfoSerializer {
    pub fn new(index: pgrx::pg_sys::Relation) -> Self {
        Self {
            index,
            term_infos: Vec::with_capacity(vocab_len() as usize),
        }
    }

    pub fn push(&mut self, term_info: PostingTermInfo) {
        self.term_infos.push(term_info);
    }

    pub fn finalize(self) -> pgrx::pg_sys::BlockNumber {
        let mut pager = VirtualPageWriter::new(self.index, PageFlags::TERM_INFO, true);
        pager.write(bytemuck::cast_slice(&self.term_infos));
        pager.finalize()
    }
}

pub struct PostingSerializer {
    index: pgrx::pg_sys::Relation,
    // block encoder
    block_encode: BlockEncode,
    prev_block_last_doc_id: u32,
    // block buffer
    doc_ids: Vec<u32>,
    term_freqs: Vec<u32>,
    // skip info writer
    skip_info_writer: Option<PageWriter>,
    // block data writer
    block_data_writer: Option<VirtualPageWriter>,
}

impl PostingSerializer {
    pub fn new(index: pgrx::pg_sys::Relation, option: EncodeOption) -> Self {
        Self {
            index,
            block_encode: BlockEncode::new(option),
            prev_block_last_doc_id: 0,
            doc_ids: Vec::with_capacity(128),
            term_freqs: Vec::with_capacity(128),
            skip_info_writer: None,
            block_data_writer: None,
        }
    }

    pub fn new_term(&mut self) {
        self.skip_info_writer = Some(PageWriter::new(self.index, PageFlags::SKIP_INFO, true));
        self.block_data_writer = Some(VirtualPageWriter::new(
            self.index,
            PageFlags::BLOCK_DATA,
            true,
        ));
        self.prev_block_last_doc_id = 0;
    }

    pub fn write_doc(&mut self, doc_id: u32, freq: u32) {
        self.doc_ids.push(doc_id);
        self.term_freqs.push(freq);
    }

    // return (skip_info_blkno, skip_info_last_blkno, block_data_blkno)
    pub fn close_term<R: FieldNormRead>(
        &mut self,
        bm25_weight: &Bm25Weight,
        fieldnorm_reader: &R,
    ) -> (u32, u32, u32) {
        if !self.doc_ids.is_empty() {
            let (blockwand_tf, blockwand_fieldnorm_id) = blockwand_max_calculate(
                &self.doc_ids,
                &self.term_freqs,
                bm25_weight,
                fieldnorm_reader,
            );
            let skip_block = SkipBlock {
                last_doc: *self.doc_ids.last().unwrap(),
                doc_cnt: self.doc_ids.len().try_into().unwrap(),
                blockwand_tf,
                size: 0,
                blockwand_fieldnorm_id,
                flag: SkipBlockFlags::UNFULLED,
            };
            self.skip_info_writer
                .as_mut()
                .unwrap()
                .write(bytemuck::bytes_of(&skip_block));
        }

        let skip_info_last_blkno = self.skip_info_writer.as_ref().unwrap().blkno();
        let skip_info_blkno = self.skip_info_writer.take().unwrap().finalize();
        let block_data_blkno = self.block_data_writer.take().unwrap().finalize();
        self.doc_ids.clear();
        self.term_freqs.clear();
        (skip_info_blkno, skip_info_last_blkno, block_data_blkno)
    }

    pub fn flush_block(&mut self, blockwand_tf: u32, blockwand_fieldnorm_id: u8) {
        let offset = NonZeroU32::new(self.prev_block_last_doc_id);
        self.prev_block_last_doc_id = *self.doc_ids.last().unwrap();
        let data = self
            .block_encode
            .encode(offset, &mut self.doc_ids, &mut self.term_freqs);

        let page_changed = self
            .block_data_writer
            .as_mut()
            .unwrap()
            .write_vectorized_no_cross(&[data]);

        let mut flag = SkipBlockFlags::empty();
        if page_changed {
            flag |= SkipBlockFlags::PAGE_CHANGED;
        }
        let doc_cnt = self.doc_ids.len().try_into().unwrap();
        let skip_block = SkipBlock {
            last_doc: self.prev_block_last_doc_id,
            doc_cnt,
            blockwand_tf,
            size: data.len().try_into().unwrap(),
            blockwand_fieldnorm_id,
            flag,
        };
        self.skip_info_writer
            .as_mut()
            .unwrap()
            .write(bytemuck::bytes_of(&skip_block));

        self.doc_ids.clear();
        self.term_freqs.clear();
    }

    pub fn unflushed_data(&self) -> (&[u32], &[u32]) {
        (&self.doc_ids, &self.term_freqs)
    }

    pub fn prev_block_last_doc_id(&self) -> u32 {
        self.prev_block_last_doc_id
    }
}

fn blockwand_max_calculate<R: FieldNormRead>(
    docids: &[u32],
    freqs: &[u32],
    bm25_weight: &Bm25Weight,
    fieldnorm_reader: &R,
) -> (u32, u8) {
    let mut max_score = 0.0;
    let mut max_fieldnorm_id = 0;
    let mut max_tf = 0;
    for (&doc_id, &freq) in docids.iter().zip(freqs.iter()) {
        let fieldnorm_id = fieldnorm_reader.read(doc_id);
        let fieldnorm = id_to_fieldnorm(fieldnorm_id);
        let score = bm25_weight.score(fieldnorm, freq);
        if score > max_score {
            max_score = score;
            max_fieldnorm_id = fieldnorm_id;
            max_tf = freq;
        }
    }
    (max_tf, max_fieldnorm_id)
}

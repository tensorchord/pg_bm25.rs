use crate::{
    page::{page_read, page_write, PageFlags, PageWriter, VirtualPageWriter},
    segment::{
        field_norm::{id_to_fieldnorm, FieldNormRead, FieldNormReader, MAX_FIELD_NORM},
        meta::MetaPageData,
        posting::SkipBlockFlags,
    },
    utils::compress_block::{BlockDecoder, BlockEncoder},
    weight::{idf, Bm25Weight},
};

use super::{PostingTermInfo, PostingTermInfoReader, SkipBlock, COMPRESSION_BLOCK_SIZE};

pub trait InvertedSerialize {
    fn new_term(&mut self, doc_count: u32);
    fn write_doc(&mut self, doc_id: u32, freq: u32);
    fn close_term(&mut self);
}

pub struct InvertedSerializer<R: FieldNormRead> {
    postings_serializer: PostingSerializer<R>,
    term_info_serializer: PostingTermInfoSerializer,
    current_term_info: PostingTermInfo,
}

impl<R: FieldNormRead> InvertedSerializer<R> {
    pub fn new(
        index: pgrx::pg_sys::Relation,
        doc_cnt: u32,
        avgdl: f32,
        fieldnorm_reader: R,
    ) -> Self {
        let postings_serializer = PostingSerializer::new(index, doc_cnt, avgdl, fieldnorm_reader);
        let term_info_serializer = PostingTermInfoSerializer::new(index);
        Self {
            postings_serializer,
            term_info_serializer,
            current_term_info: PostingTermInfo::empty(),
        }
    }

    /// return term_info_blkno
    pub fn finalize(self) -> pgrx::pg_sys::BlockNumber {
        self.term_info_serializer.finalize()
    }
}

impl<R: FieldNormRead> InvertedSerialize for InvertedSerializer<R> {
    fn new_term(&mut self, doc_count: u32) {
        if doc_count != 0 {
            self.postings_serializer.new_term(doc_count);
        }
        self.current_term_info = PostingTermInfo {
            doc_count,
            ..PostingTermInfo::empty()
        };
    }

    fn write_doc(&mut self, doc_id: u32, freq: u32) {
        self.postings_serializer.write_doc(doc_id, freq);
    }

    fn close_term(&mut self) {
        if self.current_term_info.doc_count != 0 {
            let (skip_info_blkno, skip_info_last_blkno, block_data_blkno) =
                self.postings_serializer.close_term();
            self.current_term_info.skip_info_blkno = skip_info_blkno;
            self.current_term_info.skip_info_last_blkno = skip_info_last_blkno;
            self.current_term_info.block_data_blkno = block_data_blkno;
        }
        self.term_info_serializer.push(self.current_term_info);
    }
}

enum AppendState {
    Empty,
    New,
    Append,
}

pub struct InvertedAppender {
    postings_serializer: PostingSerializer<FieldNormReader>,
    term_info_reader: PostingTermInfoReader,
    current_term_info: PostingTermInfo,
    term_id: u32,
    state: AppendState,
}

impl InvertedAppender {
    pub fn new(index: pgrx::pg_sys::Relation, meta: &MetaPageData) -> Self {
        let doc_cnt = meta.doc_cnt;
        let avgdl = meta.avgdl();
        let fieldnorm_reader = FieldNormReader::new(index, meta.field_norm_blkno);
        let postings_serializer = PostingSerializer::new(index, doc_cnt, avgdl, fieldnorm_reader);
        let term_info_reader = PostingTermInfoReader::new(index, meta.sealed_segment);
        Self {
            postings_serializer,
            term_info_reader,
            current_term_info: PostingTermInfo::empty(),
            term_id: 0,
            state: AppendState::Empty,
        }
    }
}

impl InvertedSerialize for InvertedAppender {
    fn new_term(&mut self, doc_count: u32) {
        if doc_count != 0 {
            let term_info = self.term_info_reader.read(self.term_id);
            if term_info.doc_count != 0 {
                self.postings_serializer.open_append(term_info, doc_count);
                self.current_term_info = term_info;
                self.current_term_info.doc_count += doc_count;
                self.state = AppendState::Append;
            } else {
                self.postings_serializer.new_term(doc_count);
                self.current_term_info = PostingTermInfo {
                    doc_count,
                    ..PostingTermInfo::empty()
                };
                self.state = AppendState::New;
            }
        } else {
            self.state = AppendState::Empty;
        }
    }

    fn write_doc(&mut self, doc_id: u32, freq: u32) {
        self.postings_serializer.write_doc(doc_id, freq);
    }

    fn close_term(&mut self) {
        match self.state {
            AppendState::Empty => {}
            AppendState::New => {
                let (skip_info_blkno, skip_info_last_blkno, block_data_blkno) =
                    self.postings_serializer.close_term();
                self.current_term_info.skip_info_blkno = skip_info_blkno;
                self.current_term_info.skip_info_last_blkno = skip_info_last_blkno;
                self.current_term_info.block_data_blkno = block_data_blkno;
                self.term_info_reader
                    .write(self.term_id, self.current_term_info);
            }
            AppendState::Append => {
                let (_, skip_info_last_blkno, _) = self.postings_serializer.close_term();
                self.current_term_info.skip_info_last_blkno = skip_info_last_blkno;
                self.term_info_reader
                    .write(self.term_id, self.current_term_info);
            }
        }
        self.term_id += 1;
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
            term_infos: Vec::new(),
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

struct PostingSerializer<R: FieldNormRead> {
    index: pgrx::pg_sys::Relation,
    // block encoder
    doc_id_encoder: BlockEncoder,
    term_freq_encoder: BlockEncoder,
    last_doc_id: u32,
    // block buffer
    doc_ids: [u32; COMPRESSION_BLOCK_SIZE],
    term_freqs: [u32; COMPRESSION_BLOCK_SIZE],
    block_size: usize,
    // skip info writer
    skip_info_writer: Option<PageWriter>,
    // block data writer
    block_data_writer: Option<VirtualPageWriter>,
    is_new_page: bool,
    // block wand helper
    avg_dl: f32,
    doc_cnt: u32,
    bm25_weight: Option<Bm25Weight>,
    fieldnorm_reader: R,
}

impl<R: FieldNormRead> PostingSerializer<R> {
    pub fn new(
        index: pgrx::pg_sys::Relation,
        doc_cnt: u32,
        avg_dl: f32,
        fieldnorm_reader: R,
    ) -> Self {
        Self {
            index,
            doc_id_encoder: BlockEncoder::new(),
            term_freq_encoder: BlockEncoder::new(),
            last_doc_id: 0,
            doc_ids: [0; COMPRESSION_BLOCK_SIZE],
            term_freqs: [0; COMPRESSION_BLOCK_SIZE],
            block_size: 0,
            skip_info_writer: None,
            block_data_writer: None,
            is_new_page: false,
            avg_dl,
            doc_cnt,
            bm25_weight: None,
            fieldnorm_reader,
        }
    }

    pub fn open_append(&mut self, term_info: PostingTermInfo, append_doc_count: u32) {
        let doc_count = term_info.doc_count + append_doc_count;
        self.block_size = term_info.doc_count as usize % COMPRESSION_BLOCK_SIZE;
        if self.block_size != 0 {
            let mut skip_info_last_page = page_write(self.index, term_info.skip_info_last_blkno);
            let data = skip_info_last_page.data();
            let offset = data.len() - std::mem::size_of::<SkipBlock>();
            let skip_info_last =
                unsafe { (&data[offset..] as *const _ as *const SkipBlock).read() };
            skip_info_last_page.header.pd_lower -= std::mem::size_of::<SkipBlock>() as u16;
            drop(skip_info_last_page);

            let block_size = skip_info_last.block_size();
            let mut decoder = BlockDecoder::new();

            let mut block_data_writer =
                VirtualPageWriter::open(self.index, term_info.block_data_blkno, false);
            let data_page = block_data_writer.data_page();
            let data = data_page.data();
            let mut last_block = &data[data.len() - block_size..];
            self.last_doc_id = u32::from_le_bytes(last_block[..4].try_into().unwrap());
            last_block = &last_block[4..];
            let offset = decoder.decompress_vint_sorted(
                last_block,
                self.last_doc_id,
                self.block_size as u32,
            );
            self.doc_ids[..self.block_size].copy_from_slice(decoder.output());
            last_block = &last_block[offset..];
            decoder.decompress_vint_unsorted(last_block, self.block_size as u32);
            self.term_freqs[..self.block_size].copy_from_slice(decoder.output());
            self.term_freqs[..self.block_size]
                .iter_mut()
                .for_each(|x| *x += 1);
            data_page.header.pd_lower -= block_size as u16;
            let is_new_page = data_page.data().is_empty();

            self.skip_info_writer = Some(PageWriter::open(
                self.index,
                term_info.skip_info_last_blkno,
                false,
            ));
            self.block_data_writer = Some(block_data_writer);
            self.is_new_page = is_new_page;
        } else {
            let skip_info_last_page = page_read(self.index, term_info.skip_info_last_blkno);
            let data = skip_info_last_page.data();
            let offset = data.len() - std::mem::size_of::<SkipBlock>();
            let skip_info_last =
                unsafe { (&data[offset..] as *const _ as *const SkipBlock).read() };
            drop(skip_info_last_page);
            self.last_doc_id = skip_info_last.last_doc;

            self.skip_info_writer = Some(PageWriter::open(
                self.index,
                term_info.skip_info_last_blkno,
                false,
            ));
            self.block_data_writer = Some(VirtualPageWriter::open(
                self.index,
                term_info.block_data_blkno,
                false,
            ));
            self.is_new_page = false;
        }

        let idf = idf(self.doc_cnt, doc_count);
        self.bm25_weight = Some(Bm25Weight::new(1, idf, self.avg_dl));
    }

    pub fn new_term(&mut self, doc_count: u32) {
        self.skip_info_writer = Some(PageWriter::new(self.index, PageFlags::SKIP_INFO, true));
        self.block_data_writer = Some(VirtualPageWriter::new(
            self.index,
            PageFlags::BLOCK_DATA,
            true,
        ));
        self.last_doc_id = 0;
        self.is_new_page = false;
        let idf = idf(self.doc_cnt, doc_count);
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

    // return (skip_info_blkno, skip_info_last_blkno, block_data_blkno)
    pub fn close_term(&mut self) -> (u32, u32, u32) {
        if self.block_size > 0 {
            if self.block_size == COMPRESSION_BLOCK_SIZE {
                self.flush_block();
            } else {
                self.flush_block_unfull();
            }
        }
        let skip_info_last_blkno = self.skip_info_writer.as_ref().unwrap().blkno();
        let skip_info_blkno = self.skip_info_writer.take().unwrap().finalize();
        let block_data_blkno = self.block_data_writer.take().unwrap().finalize();
        self.bm25_weight = None;
        (skip_info_blkno, skip_info_last_blkno, block_data_blkno)
    }

    fn flush_block(&mut self) {
        assert!(self.block_size == COMPRESSION_BLOCK_SIZE);

        let (blockwand_tf, blockwand_fieldnorm_id) = self.block_wand();

        // doc_id
        let (docid_bits, docid_block) = self
            .doc_id_encoder
            .compress_block_sorted(&self.doc_ids[..self.block_size], self.last_doc_id);
        self.last_doc_id = self.doc_ids[self.block_size - 1];

        // term_freq
        for i in 0..self.block_size {
            self.term_freqs[i] -= 1;
        }
        let (tf_bits, term_freq_block) = self
            .term_freq_encoder
            .compress_block_unsorted(&self.term_freqs[..self.block_size]);

        let change_page = self
            .block_data_writer
            .as_mut()
            .unwrap()
            .write_vectorized_no_cross(&[docid_block, term_freq_block]);

        let mut flag = SkipBlockFlags::empty();
        if change_page {
            flag |= SkipBlockFlags::PAGE_CHANGED;
        }
        if self.is_new_page {
            flag |= SkipBlockFlags::PAGE_CHANGED;
            self.is_new_page = false;
        }
        let skip_block = SkipBlock {
            last_doc: self.last_doc_id,
            docid_bits,
            tf_bits,
            blockwand_tf,
            blockwand_fieldnorm_id,
            flag,
        };
        self.skip_info_writer
            .as_mut()
            .unwrap()
            .write(bytemuck::cast_slice(&[skip_block]));

        self.block_size = 0;
    }

    fn flush_block_unfull(&mut self) {
        assert!(self.block_size > 0);

        let (blockwand_tf, blockwand_fieldnorm_id) = self.block_wand();

        // doc_id
        let docid_block = self
            .doc_id_encoder
            .compress_vint_sorted(&self.doc_ids[..self.block_size], self.last_doc_id);
        let prev_last_doc_id = self.last_doc_id;
        self.last_doc_id = self.doc_ids[self.block_size - 1];

        // term_freq
        for i in 0..self.block_size {
            self.term_freqs[i] -= 1;
        }
        let term_freq_block = self
            .term_freq_encoder
            .compress_vint_unsorted(&self.term_freqs[..self.block_size]);

        let change_page = self
            .block_data_writer
            .as_mut()
            .unwrap()
            .write_vectorized_no_cross(&[
                &prev_last_doc_id.to_le_bytes(),
                docid_block,
                term_freq_block,
            ]);

        let block_len = std::mem::size_of::<u32>() + docid_block.len() + term_freq_block.len();
        let mut flag = SkipBlockFlags::UNFULLED;
        if change_page {
            flag |= SkipBlockFlags::PAGE_CHANGED;
        }
        if self.is_new_page {
            flag |= SkipBlockFlags::PAGE_CHANGED;
            self.is_new_page = false;
        }
        let skip_block = SkipBlock {
            last_doc: self.last_doc_id,
            docid_bits: (block_len >> 8) as u8,
            tf_bits: block_len as u8,
            blockwand_tf,
            blockwand_fieldnorm_id,
            flag,
        };
        self.skip_info_writer
            .as_mut()
            .unwrap()
            .write(bytemuck::cast_slice(&[skip_block]));

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
            let fieldnorm_id = self.fieldnorm_reader.read(doc_id);
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

use std::io::Read;

use aligned_vec::{AVec, ConstAlign};

use crate::{
    bm25weight::Bm25Weight,
    field_norm::id_to_fieldnorm,
    page::{ContinousPageReader, MetaPageData, PageReader},
    utils::compress_block::{compressed_block_size, BlockDecoder},
};

use super::{SkipBlock, TermInfo, COMPRESSION_BLOCK_SIZE};

pub struct InvertedReader {
    index: pgrx::pg_sys::Relation,
    term_dict_reader: TermDictReader,
    term_info_reader: TermInfoReader,
}

impl InvertedReader {
    pub fn new(index: pgrx::pg_sys::Relation, meta: &MetaPageData) -> anyhow::Result<Self> {
        let term_dict_reader = TermDictReader::new(index, meta.term_dict_blkno)?;
        let term_info_reader = TermInfoReader::new(index, meta.term_info_blkno);
        Ok(Self {
            index,
            term_dict_reader,
            term_info_reader,
        })
    }

    pub fn get_posting_reader(&self, term: &[u8]) -> anyhow::Result<PostingReader> {
        let term_id = self
            .term_dict_reader
            .get(term)
            .ok_or_else(|| anyhow::anyhow!("term not found"))?;
        let term_info = self.term_info_reader.read(term_id);
        PostingReader::new(self.index, term_info)
    }
}

pub struct TermDictReader {
    map: fst::Map<Vec<u8>>,
}

impl TermDictReader {
    pub fn new(
        index: pgrx::pg_sys::Relation,
        blkno: pgrx::pg_sys::BlockNumber,
    ) -> anyhow::Result<Self> {
        let mut pager = PageReader::new(index, blkno);
        let mut buf = Vec::new();
        pager.read_to_end(&mut buf)?;
        let map = fst::Map::new(buf)?;
        Ok(Self { map })
    }

    pub fn get(&self, key: &[u8]) -> Option<u32> {
        self.map.get(key).map(|v| v.try_into().unwrap())
    }
}

pub struct TermInfoReader(ContinousPageReader<TermInfo>);

impl TermInfoReader {
    pub fn new(index: pgrx::pg_sys::Relation, blkno: pgrx::pg_sys::BlockNumber) -> Self {
        Self(ContinousPageReader::new(index, blkno))
    }

    pub fn read(&self, term_ord: u32) -> TermInfo {
        self.0.read(term_ord)
    }
}

pub struct PostingReader {
    doc_cnt: u32,
    data: AVec<u8, ConstAlign<4>>,
}

impl PostingReader {
    pub fn new(index: pgrx::pg_sys::Relation, term_info: TermInfo) -> anyhow::Result<Self> {
        let mut reader = PageReader::new(index, term_info.postings_blkno);
        let mut data = AVec::new(4);
        reader.read_to_end_aligned(&mut data)?;
        Ok(Self {
            doc_cnt: term_info.docs,
            data,
        })
    }

    pub fn get_posting(&self) -> Posting {
        Posting::new(self.doc_cnt, &self.data)
    }

    pub fn doc_cnt(&self) -> u32 {
        self.doc_cnt
    }
}

pub struct Posting<'a> {
    skip_blocks: &'a [SkipBlock],
    posting_data: &'a [u8],
    // decoders
    doc_decoder: BlockDecoder,
    freq_decoder: BlockDecoder,
    // skip cursor
    cur_block: usize,
    block_offset: usize,
    bytes_offset: usize,
    remain_doc_cnt: u32,
    // block state
    block_decoded: bool,
}

impl<'a> Posting<'a> {
    pub fn new(doc_cnt: u32, mut data: &'a [u8]) -> Self {
        let block_cnt = u32::from_le_bytes(data[..4].try_into().unwrap());
        data = &data[4..];
        let (skip_block_data, posting_data) =
            data.split_at(block_cnt as usize * std::mem::size_of::<SkipBlock>());
        let skip_blocks = bytemuck::cast_slice(skip_block_data);
        Self {
            skip_blocks,
            posting_data,
            doc_decoder: BlockDecoder::new(),
            freq_decoder: BlockDecoder::new(),
            cur_block: 0,
            block_offset: 0,
            bytes_offset: 0,
            remain_doc_cnt: doc_cnt,
            block_decoded: false,
        }
    }

    // update skip cursor to fetch the next block's skip data. If you want to read the block, call decode_block
    pub fn advance_block(&mut self) -> bool {
        assert!(!self.completed());
        self.cur_block += 1;
        self.block_offset = 0;
        self.block_decoded = false;
        self.remain_doc_cnt -= std::cmp::min(COMPRESSION_BLOCK_SIZE as u32, self.remain_doc_cnt);
        self.bytes_offset += compressed_block_size(self.skip_blocks[self.cur_block - 1].docid_bits)
            + compressed_block_size(self.skip_blocks[self.cur_block - 1].tf_bits);
        if self.completed() {
            return false;
        }
        true
    }

    // update block cursor to read next doc
    pub fn advance_cur(&mut self) -> bool {
        assert!(self.block_decoded);
        if self.block_offset < COMPRESSION_BLOCK_SIZE.min(self.remain_doc_cnt as usize) {
            self.block_offset += 1;
        }
        if self.block_offset == COMPRESSION_BLOCK_SIZE.min(self.remain_doc_cnt as usize) {
            return false;
        }
        true
    }

    pub fn shallow_seek(&mut self, doc_id: u32) -> bool {
        assert!(!self.completed());
        while self.skip_blocks[self.cur_block].last_doc < doc_id {
            if !self.advance_block() {
                return false;
            }
        }
        true
    }

    pub fn doc_id(&self) -> u32 {
        assert!(self.block_decoded);
        self.doc_decoder.output()[self.block_offset]
    }

    pub fn term_freq(&self) -> u32 {
        assert!(self.block_decoded);
        self.freq_decoder.output()[self.block_offset]
    }

    pub fn block_max_score(&self, bm25_weight: &Bm25Weight) -> f32 {
        if self.completed() {
            return 0.0;
        }
        let fieldnorm_id = self.skip_blocks[self.cur_block].blockwand_fieldnorm_id;
        let fieldnorm = id_to_fieldnorm(fieldnorm_id);
        let tf = self.skip_blocks[self.cur_block].blockwand_tf;
        bm25_weight.score(tf, fieldnorm)
    }

    pub fn last_doc_in_block(&self) -> u32 {
        assert!(!self.completed());
        self.skip_blocks[self.cur_block].last_doc
    }

    fn completed(&self) -> bool {
        self.remain_doc_cnt == 0
    }

    pub fn decode_block(&mut self) {
        assert!(
            !self.completed() && !self.block_decoded,
            "self.completed: {}, self.block_decoded: {}",
            self.completed(),
            self.block_decoded
        );
        let skip = &self.skip_blocks[self.cur_block];
        let last_doc = if self.cur_block == 0 {
            0
        } else {
            self.skip_blocks[self.cur_block - 1].last_doc
        };

        if self.remain_doc_cnt < COMPRESSION_BLOCK_SIZE as u32 {
            let bytes = self.doc_decoder.decompress_vint_sorted(
                &self.posting_data[self.bytes_offset..],
                last_doc,
                self.remain_doc_cnt,
            );
            self.freq_decoder.decompress_vint_unsorted(
                &self.posting_data[(self.bytes_offset + bytes)..],
                self.remain_doc_cnt,
            );
            self.freq_decoder.output_mut().iter_mut().for_each(|v| *v += 1);
        } else {
            let bytes = self.doc_decoder.decompress_block_sorted(
                &self.posting_data[self.bytes_offset..],
                skip.docid_bits,
                last_doc,
            );
            self.freq_decoder.decompress_block_unsorted(
                &self.posting_data[(self.bytes_offset + bytes)..],
                skip.tf_bits,
            );
            self.freq_decoder.output_mut().iter_mut().for_each(|v| *v += 1);
        }
        self.block_decoded = true;
    }
}

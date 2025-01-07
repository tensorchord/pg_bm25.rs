use std::num::NonZeroU32;

use bitvec::{field::BitField, slice::BitSlice};

use super::{BlockDecodeTrait, BlockEncodeTrait};

pub struct EliasFanoEncode {
    output: Vec<u64>,
}

impl EliasFanoEncode {
    pub fn new() -> Self {
        Self { output: Vec::new() }
    }
}

impl BlockEncodeTrait for EliasFanoEncode {
    fn encode(
        &mut self,
        last_docid: Option<NonZeroU32>,
        docids: &mut [u32],
        freqs: &mut [u32],
    ) -> &[u8] {
        let offset = last_docid.map(|v| v.get() + 1).unwrap_or(0);
        docids.iter_mut().for_each(|v| *v -= offset);
        freqs.iter_mut().for_each(|v| *v -= 1);
        let freq_max_bits = 32 - freqs.iter().max().unwrap().leading_zeros() as u8;
        let universe = ((*docids.last().unwrap() as u64) << freq_max_bits
            | (*freqs.last().unwrap() as u64))
            + 1;

        self.output.clear();
        self.output
            .extend_from_slice(&[freq_max_bits as u64, universe]);

        let iter = docids
            .iter()
            .zip(freqs.iter())
            .map(|(docid, freq)| (*docid as u64) << freq_max_bits | (*freq as u64));
        compress_elias_fano(&mut self.output, universe, docids.len() as u64, iter);

        bytemuck::cast_slice(self.output.as_slice())
    }
}

pub struct EliasFanoDecode {
    data: Vec<u64>,
    offsets: Box<EliasFanoOffsets>,
    pos: u64,
    value: u64,
    // high enumerator
    high_pos: u64,
    high_buf: u64,
    docid_offset: u32,
    freq_max_bits: u8,
}

impl EliasFanoDecode {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            offsets: Box::new(EliasFanoOffsets::default()),
            pos: 0,
            value: 0,
            high_pos: 0,
            high_buf: 0,
            docid_offset: 0,
            freq_max_bits: 0,
        }
    }
}

impl BlockDecodeTrait for EliasFanoDecode {
    fn decode(&mut self, data: &[u8], last_docid: Option<NonZeroU32>, doc_cnt: u32) {
        let docid_offset = last_docid.map(|v| v.get() + 1).unwrap_or(0);
        self.docid_offset = docid_offset;
        let freq_max_bits: u8 = (*bytemuck::from_bytes::<u64>(&data[..8]))
            .try_into()
            .unwrap();
        self.freq_max_bits = freq_max_bits;
        let universe = *bytemuck::from_bytes::<u64>(&data[8..16]);
        *self.offsets = EliasFanoOffsets::new(universe, doc_cnt as u64);

        let data_len = self.offsets.end.div_ceil(64) as usize * 8;
        self.data.clear();
        self.data
            .extend_from_slice(bytemuck::cast_slice(&data[16..][..data_len]));

        self.pos = 0;
        self.value = 0;

        self.set_high_enum(self.offsets.higher_bits_offset);
        self.value = self.read_next();
    }

    fn next(&mut self) -> bool {
        self.pos += 1;

        if self.pos == self.len() {
            return false;
        }

        self.value = self.read_next();
        true
    }

    fn seek(&mut self, target: u32) -> bool {
        if self.docid() >= target {
            return true;
        }
        self.next_geq(((target - self.docid_offset) as u64) << self.freq_max_bits)
    }

    fn docid(&self) -> u32 {
        (self.value >> self.freq_max_bits) as u32 + self.docid_offset
    }

    fn freq(&self) -> u32 {
        (self.value & ((1 << self.freq_max_bits) - 1)) as u32 + 1
    }
}

impl EliasFanoDecode {
    const LINEAR_SCAN_THRESHOLD: u64 = 8;

    fn len(&self) -> u64 {
        self.offsets.len
    }

    fn read_next(&mut self) -> u64 {
        assert!(self.pos < self.len());
        let high = self.high_next() - self.offsets.higher_bits_offset;
        ((high - self.pos - 1) << self.offsets.lower_bits) | self.read_low()
    }

    fn read_low(&self) -> u64 {
        let low = self.offsets.lower_bits_offset + self.pos * self.offsets.lower_bits;
        get_word56(&self.data, low) & self.offsets.mask
    }

    fn next_geq(&mut self, target: u64) -> bool {
        let high_target = target >> self.offsets.lower_bits;
        let cur_high = self.value >> self.offsets.lower_bits;
        let high_diff = high_target - cur_high;

        if high_diff <= Self::LINEAR_SCAN_THRESHOLD {
            loop {
                if !self.next() {
                    return false;
                }

                if self.value >= target {
                    return true;
                }
            }
        }

        self.slow_next_geq(target)
    }

    fn slow_next_geq(&mut self, target: u64) -> bool {
        if target >= self.offsets.universe {
            return false;
        }

        let high_target = target >> self.offsets.lower_bits;
        let cur_high = self.value >> self.offsets.lower_bits;
        let high_diff = high_target - cur_high;

        let to_skip = if high_diff >> self.offsets.log_sampling0 > 0 {
            let ptr0 = high_target >> self.offsets.log_sampling0;
            let high_pos = self.pointer0(ptr0);
            let high_rank0 = ptr0 << self.offsets.log_sampling0;
            self.set_high_enum(self.offsets.higher_bits_offset + high_pos);
            high_target - high_rank0
        } else {
            high_diff
        };
        self.high_skip0(to_skip);
        self.pos = self.high_pos - self.offsets.higher_bits_offset - high_target;

        loop {
            if self.pos == self.len() {
                return false;
            }

            self.value = self.read_next();
            if self.value >= target {
                return true;
            }

            self.pos += 1;
        }
    }

    // high enumerator
    fn high_next(&mut self) -> u64 {
        let mut buf = self.high_buf;
        while buf == 0 {
            self.high_pos += 64;
            buf = self.data[self.high_pos as usize / 64];
        }
        let pos_in_word = buf.trailing_zeros() as u64;
        self.high_buf = buf & (buf - 1);
        self.high_pos = (self.high_pos & !63) + pos_in_word;
        self.high_pos
    }

    fn set_high_enum(&mut self, pos: u64) {
        self.high_pos = pos;
        self.high_buf = self.data[self.high_pos as usize / 64];
        self.high_buf &= (-1i64 as u64) << (self.high_pos % 64);
    }

    fn high_skip0(&mut self, to_skip: u64) {
        let mut skipped = 0;
        let mut pos_in_word = self.high_pos % 64;
        let mut buf = !self.high_buf & ((-1i64 as u64) << pos_in_word);
        let mut w;
        while {
            w = buf.count_ones() as u64;
            skipped + w <= to_skip
        } {
            skipped += w;
            self.high_pos += 64;
            buf = !self.data[self.high_pos as usize / 64];
        }
        pos_in_word = broadword::select1((to_skip - skipped) as usize, buf).unwrap() as u64;
        self.high_buf = !buf & (-1i64 as u64) << pos_in_word;
        self.high_pos = (self.high_pos & !63) + pos_in_word;
    }

    // pointer
    fn pointer0(&self, i: u64) -> u64 {
        if i == 0 {
            return 0;
        }
        let pos = (i - 1) * self.offsets.pointer_size;
        get_word56(&self.data, pos) & ((1 << self.offsets.pointer_size) - 1)
    }
}

#[derive(Debug, Default)]
struct EliasFanoOffsets {
    universe: u64,
    len: u64,
    log_sampling0: u64,
    lower_bits: u64,
    mask: u64,
    higher_bits_length: u64,
    pointer_size: u64,
    higher_bits_offset: u64,
    lower_bits_offset: u64,
    end: u64,
}

impl EliasFanoOffsets {
    const EF_LOG_SAMPLING0: u64 = 9;

    fn new(universe: u64, len: u64) -> Self {
        let log_sampling0 = Self::EF_LOG_SAMPLING0;
        let lower_bits = if universe > len {
            (universe / len).ilog2() as u64
        } else {
            0
        };
        let mask = (1 << lower_bits) - 1;
        let higher_bits_length = len + (universe >> lower_bits) + 2;
        let pointer_size = ceil_log2(higher_bits_length);
        let pointers0 = (higher_bits_length - len) >> log_sampling0;
        let higher_bits_offset = pointers0 * pointer_size;
        let lower_bits_offset = higher_bits_offset + higher_bits_length;
        let end = lower_bits_offset + len * lower_bits;

        Self {
            universe,
            len,
            log_sampling0,
            lower_bits,
            mask,
            higher_bits_length,
            pointer_size,
            higher_bits_offset,
            lower_bits_offset,
            end,
        }
    }
}

fn compress_elias_fano(
    buffer: &mut Vec<u64>,
    universe: u64,
    len: u64,
    iter: impl Iterator<Item = u64>,
) {
    let offsets = EliasFanoOffsets::new(universe, len);

    let prev_len = buffer.len();
    buffer.resize(prev_len + offsets.end.div_ceil(64) as usize, 0);
    let buffer = BitSlice::<u64>::from_slice_mut(&mut buffer[prev_len..]);

    let set_ptr0s = |buffer: &mut BitSlice<u64>, mut begin: u64, mut end: u64, rank_end: u64| {
        begin -= rank_end;
        end -= rank_end;
        let mut ptr0 = begin.div_ceil(1 << offsets.log_sampling0);
        while ptr0 << offsets.log_sampling0 < end {
            if ptr0 == 0 {
                ptr0 = 1;
                continue;
            }
            let ptr0_offset = (ptr0 - 1) * offsets.pointer_size;
            assert!(ptr0_offset + offsets.pointer_size <= offsets.higher_bits_offset);
            buffer[ptr0_offset as usize..(ptr0_offset + offsets.pointer_size) as usize]
                .store((ptr0 << offsets.log_sampling0) + rank_end);
            ptr0 += 1;
        }
    };

    let mut last: u64 = 0;
    let mut last_high: u64 = 0;
    for (i, value) in iter.enumerate() {
        if i != 0 && value < last {
            panic!("values must be sorted in compress_elias_fano");
        }
        if value >= universe {
            panic!(
                "value({}) must be less than universe({}) in compress_elias_fano",
                value, universe
            );
        }

        let high = (value >> offsets.lower_bits) + i as u64 + 1;
        let low = value & offsets.mask;

        buffer.set((offsets.higher_bits_offset + high) as usize, true);

        let low_offset = offsets.lower_bits_offset + i as u64 * offsets.lower_bits;
        assert!(low_offset + offsets.lower_bits <= offsets.end);
        buffer[low_offset as usize..(low_offset + offsets.lower_bits) as usize].store(low);

        // set 0 pointers
        set_ptr0s(buffer, last_high + 1, high, i as u64);

        last_high = high;
        last = value;
    }

    set_ptr0s(buffer, last_high + 1, offsets.higher_bits_length, len);
}

// floor(log2(x)) == ceil(log2(x + 1)) - 1
fn ceil_log2(x: u64) -> u64 {
    if x < 2 {
        return 0;
    }
    (x - 1).ilog2() as u64 + 1
}

// fast version to retrieve a dword from bitvector, at least 56 bits
fn get_word56(buf: &[u64], pos: u64) -> u64 {
    let buf: &[u8] = bytemuck::cast_slice(buf);
    let buf = &buf[(pos as usize / 8)..];
    let mut number_buf = [0u8; 8];
    let len = std::cmp::min(8, buf.len());
    number_buf[..len].copy_from_slice(&buf[..len]);
    let res = u64::from_le_bytes(number_buf);
    res >> (pos % 8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next() {
        let mut encoder = EliasFanoEncode::new();
        let mut decoder = EliasFanoDecode::new();

        let mut docids = rand::seq::index::sample(&mut rand::thread_rng(), 10000, 1000)
            .into_iter()
            .map(|x| x as u32 + 10)
            .collect::<Vec<_>>();
        docids.sort_unstable();
        let freqs = (0..1000)
            .map(|_| rand::random::<u32>() % 1000 + 1)
            .collect::<Vec<_>>();
        let offset = NonZeroU32::new(9);

        println!("docids: {:?}", docids);
        println!("freqs: {:?}", freqs);

        let data = encoder.encode(offset, &mut docids.clone(), &mut freqs.clone());
        decoder.decode(data, offset, docids.len() as u32);

        for i in 0..docids.len() {
            assert_eq!(docids[i], decoder.docid());
            assert_eq!(freqs[i], decoder.freq());
            if i + 1 < docids.len() {
                assert!(decoder.next());
            } else {
                assert!(!decoder.next());
            }
        }
    }

    #[test]
    fn test_seek() {
        let mut encoder = EliasFanoEncode::new();
        let mut decoder = EliasFanoDecode::new();

        let mut docids = rand::seq::index::sample(&mut rand::thread_rng(), 10000, 1000)
            .into_iter()
            .map(|x| x as u32 + 10)
            .collect::<Vec<_>>();
        docids.sort_unstable();
        let freqs = (0..1000)
            .map(|_| rand::random::<u32>() % 1000 + 1)
            .collect::<Vec<_>>();
        let offset = NonZeroU32::new(9);

        println!("docids: {:?}", docids);
        println!("freqs: {:?}", freqs);

        let data = encoder.encode(offset, &mut docids.clone(), &mut freqs.clone());
        decoder.decode(data, offset, docids.len() as u32);

        for i in 0..docids.len() {
            assert_eq!(docids[i], decoder.docid());
            assert_eq!(freqs[i], decoder.freq());
            if i + 1 < docids.len() {
                assert!(decoder.seek(docids[i] + 1));
            } else {
                assert!(!decoder.seek(docids[i] + 1));
            }
        }
    }

    #[test]
    fn test_seek_long() {
        let mut encoder = EliasFanoEncode::new();
        let mut decoder = EliasFanoDecode::new();

        let mut docids = rand::seq::index::sample(&mut rand::thread_rng(), 10000, 1000)
            .into_iter()
            .map(|x| x as u32 + 10)
            .collect::<Vec<_>>();
        docids.sort_unstable();
        let freqs = (0..1000)
            .map(|_| rand::random::<u32>() % 1000 + 1)
            .collect::<Vec<_>>();
        let offset = NonZeroU32::new(9);

        println!("docids: {:?}", docids);
        println!("freqs: {:?}", freqs);

        let data = encoder.encode(offset, &mut docids.clone(), &mut freqs.clone());
        decoder.decode(data, offset, docids.len() as u32);

        assert_eq!(docids[0], decoder.docid());
        assert_eq!(freqs[0], decoder.freq());

        assert!(decoder.seek(docids.last().unwrap().clone()));
        assert_eq!(docids.last().unwrap().clone(), decoder.docid());
    }
}

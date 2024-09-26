#![allow(unused)]

const STOP_BIT: u8 = 128;

pub fn encode_vint32(val: u32, mut w: impl std::io::Write) -> std::io::Result<()> {
    const START_2: u64 = 1 << 7;
    const START_3: u64 = 1 << 14;
    const START_4: u64 = 1 << 21;
    const START_5: u64 = 1 << 28;

    const MASK_1: u64 = 127;
    const MASK_2: u64 = MASK_1 << 7;
    const MASK_3: u64 = MASK_2 << 7;
    const MASK_4: u64 = MASK_3 << 7;
    const MASK_5: u64 = MASK_4 << 7;

    let val = u64::from(val);
    const STOP_BIT: u64 = 128u64;
    let (res, num_bytes) = if val < START_2 {
        (val | STOP_BIT, 1)
    } else if val < START_3 {
        (
            (val & MASK_1) | ((val & MASK_2) << 1) | (STOP_BIT << (8)),
            2,
        )
    } else if val < START_4 {
        (
            (val & MASK_1) | ((val & MASK_2) << 1) | ((val & MASK_3) << 2) | (STOP_BIT << (8 * 2)),
            3,
        )
    } else if val < START_5 {
        (
            (val & MASK_1)
                | ((val & MASK_2) << 1)
                | ((val & MASK_3) << 2)
                | ((val & MASK_4) << 3)
                | (STOP_BIT << (8 * 3)),
            4,
        )
    } else {
        (
            (val & MASK_1)
                | ((val & MASK_2) << 1)
                | ((val & MASK_3) << 2)
                | ((val & MASK_4) << 3)
                | ((val & MASK_5) << 4)
                | (STOP_BIT << (8 * 4)),
            5,
        )
    };
    w.write_all(&res.to_le_bytes()[..num_bytes])
}

pub fn decode_vint32(data: &mut &[u8]) -> u32 {
    let vlen = vint32_len(data);
    let mut result = 0u32;
    let mut shift = 0u32;
    for &b in &data[..vlen] {
        result |= u32::from(b & 127u8) << shift;
        shift += 7;
    }
    *data = &data[vlen..];
    result
}

fn vint32_len(data: &[u8]) -> usize {
    for (i, &val) in data.iter().enumerate().take(5) {
        if val >= STOP_BIT {
            return i + 1;
        }
    }
    panic!("Corrupted data. Invalid VInt 32");
}

pub fn encode_vint64(mut val: u64, mut w: impl std::io::Write) -> std::io::Result<()> {
    loop {
        let b = (val & 127) as u8;
        val >>= 7;
        if val == 0 {
            w.write_all(&[b | STOP_BIT])?;
            break;
        }
        w.write_all(&[b])?;
    }
    Ok(())
}

pub fn decode_vint64(data: &mut &[u8]) -> u64 {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let b = data[0];
        *data = &data[1..];
        result |= u64::from(b & 127) << shift;
        shift += 7;
        if b >= STOP_BIT {
            break;
        }
    }
    result
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_vint32_1() {
        let mut buf = Vec::new();

        for _ in 0..100000 {
            let val = rand::random::<u32>();
            encode_vint32(val, &mut buf).unwrap();
            let mut data = buf.as_slice();
            assert_eq!(val, decode_vint32(&mut data));
            buf.clear();
        }
    }

    #[test]
    fn test_vint32_2() {
        let mut buf = Vec::new();
        let mut reference = Vec::new();

        for _ in 0..100000 {
            let val = rand::random::<u32>();
            encode_vint32(val, &mut buf).unwrap();
            reference.push(val);
        }
        let mut data = buf.as_slice();
        for i in 0..100000 {
            let val = decode_vint32(&mut data);
            assert_eq!(reference[i], val);
        }
    }

    #[test]
    fn test_vint64() {
        let mut buf = Vec::new();

        for _ in 0..100000 {
            let val = rand::random::<u64>();
            encode_vint64(val, &mut buf).unwrap();
            let mut data = buf.as_slice();
            assert_eq!(val, decode_vint64(&mut data));
            buf.clear();
        }
    }

    #[test]
    fn test_vint64_2() {
        let mut buf = Vec::new();
        let mut reference = Vec::new();

        for _ in 0..100000 {
            let val = rand::random::<u64>();
            encode_vint64(val, &mut buf).unwrap();
            reference.push(val);
        }
        let mut data = buf.as_slice();
        for i in 0..100000 {
            let val = decode_vint64(&mut data);
            assert_eq!(reference[i], val);
        }
    }
}

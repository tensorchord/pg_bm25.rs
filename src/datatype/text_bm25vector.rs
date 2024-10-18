#![allow(clippy::needless_range_loop)]
use arrayvec::ArrayVec;
use pgrx::pg_sys::Oid;
use std::ffi::{CStr, CString};
use std::fmt::Write;
use thiserror::Error;

use super::bm25vector::Bm25VectorBorrowed;
use super::memory_bm25vector::{Bm25VectorInput, Bm25VectorOutput};

#[derive(Debug, Error, PartialEq)]
pub enum ParseVectorError {
    #[error("The input string is empty.")]
    EmptyString,
    #[error("Bad character at position {position}")]
    BadCharacter { position: usize },
    #[error("Bad parentheses character '{character}'")]
    BadParentheses { character: char },
    #[error("Bad colon at position {position}")]
    BadColon { position: usize },
    #[error("Missing colon at position {position}")]
    MissingColon { position: usize },
    #[error("Too long number at position {position}")]
    TooLongNumber { position: usize },
    #[error("Too short number at position {position}")]
    TooShortNumber { position: usize },
    #[error("The sum of term frequencies is exceeding u32::MAX.")]
    TooManyDocuments,
    #[error("Bad parsing at position {position}")]
    BadParsing { position: usize },
    #[error("Indexes are not increasing at position {position}")]
    IndexNotIncreasing { position: usize },
}

/// Text format for bm25vector
/// '{term_id:tf, term_id:tf, ...}/doc_len'
/// Example: '{1:2, 3:1, 5:3}/6'
fn parse_bm25vector(input: &[u8]) -> Result<Bm25VectorOutput, ParseVectorError> {
    if input.is_empty() {
        return Err(ParseVectorError::EmptyString);
    }

    let left = 'a: {
        for position in 0..input.len() - 1 {
            match input[position] {
                b'{' => break 'a position,
                b' ' => continue,
                _ => return Err(ParseVectorError::BadCharacter { position }),
            }
        }
        return Err(ParseVectorError::BadParentheses { character: '{' });
    };
    let right = 'a: {
        for position in (1..input.len()).rev() {
            match input[position] {
                b'}' => break 'a position,
                b' ' => continue,
                _ => return Err(ParseVectorError::BadCharacter { position }),
            }
        }
        return Err(ParseVectorError::BadParentheses { character: '}' });
    };

    let mut indexes: Vec<u32> = Vec::new();
    let mut values: Vec<u32> = Vec::new();
    let mut is_index = true;
    let mut token: ArrayVec<u8, 48> = ArrayVec::new();
    for position in left + 1..right {
        let c = input[position];
        match c {
            b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-' => {
                if token.try_push(c).is_err() {
                    return Err(ParseVectorError::TooLongNumber { position });
                }
            }
            b':' => {
                if !is_index {
                    return Err(ParseVectorError::BadColon { position });
                }
                if token.is_empty() {
                    return Err(ParseVectorError::TooShortNumber { position });
                }
                let s = unsafe { std::str::from_utf8_unchecked(&token) };
                let num = s
                    .parse::<u32>()
                    .map_err(|_| ParseVectorError::BadParsing { position })?;
                if Some(num) <= indexes.last().copied() {
                    return Err(ParseVectorError::IndexNotIncreasing { position });
                }
                indexes.push(num);
                token.clear();
                is_index = false;
            }
            b',' => {
                if is_index {
                    return Err(ParseVectorError::MissingColon { position });
                }
                if token.is_empty() {
                    return Err(ParseVectorError::TooShortNumber { position });
                }
                let s = unsafe { std::str::from_utf8_unchecked(&token) };
                let num = s
                    .parse::<u32>()
                    .map_err(|_| ParseVectorError::BadParsing { position })?;
                values.push(num);
                token.clear();
                is_index = true;
            }
            b' ' => (),
            _ => return Err(ParseVectorError::BadCharacter { position }),
        }
    }

    if !token.is_empty() {
        if is_index {
            return Err(ParseVectorError::MissingColon { position: right });
        }
        let s = unsafe { std::str::from_utf8_unchecked(&token) };
        let num = s
            .parse()
            .map_err(|_| ParseVectorError::BadParsing { position: right })?;
        values.push(num);
        token.clear();
    }

    if indexes.len() != values.len() {
        return Err(ParseVectorError::TooShortNumber { position: right });
    }

    let doc_len = values.iter().map(|&v| v as usize).sum::<usize>();
    let doc_len = u32::try_from(doc_len).map_err(|_| ParseVectorError::TooManyDocuments)?;
    let vector = unsafe { Bm25VectorBorrowed::new_unchecked(doc_len, &indexes, &values) };
    Ok(Bm25VectorOutput::new(vector))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _bm25catalog_bm25vector_in(input: &CStr, _oid: Oid, _typmod: i32) -> Bm25VectorOutput {
    let input = input.to_bytes();
    match parse_bm25vector(input) {
        Ok(vector) => vector,
        Err(e) => pgrx::error!("{}", e),
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _bm25catalog_bm25vector_out(vector: Bm25VectorInput<'_>) -> CString {
    let vector = vector.as_ref();
    let mut buffer = String::new();
    buffer.push('{');
    let mut need_splitter = false;
    for (&index, &value) in vector.indexes().iter().zip(vector.values().iter()) {
        match need_splitter {
            false => {
                write!(buffer, "{}:{}", index, value).unwrap();
                need_splitter = true;
            }
            true => write!(buffer, ", {}:{}", index, value).unwrap(),
        }
    }
    buffer.push('}');
    CString::new(buffer).unwrap()
}

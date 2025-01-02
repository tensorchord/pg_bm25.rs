use std::{collections::HashMap, num::NonZero};

use pgrx::{pg_sys::panic::ErrorReportable, IntoDatum};

use crate::{
    page::{page_read, METAPAGE_BLKNO},
    segment::{meta::MetaPageData, term_stat::TermStatReader},
    token::unicode_tokenize,
    weight::bm25_score_batch,
};

use super::memory_bm25vector::{Bm25VectorInput, Bm25VectorOutput};

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
pub fn tokenize(text: &str) -> Bm25VectorOutput {
    let term_ids = crate::token::tokenize(text);
    Bm25VectorOutput::from_ids(&term_ids)
}

#[pgrx::pg_extern(stable, strict, parallel_safe)]
pub fn search_bm25query(
    target_vector: Bm25VectorInput,
    query: pgrx::composite_type!("bm25query"),
) -> f32 {
    let index_oid: pgrx::pg_sys::Oid = query
        .get_by_index(NonZero::new(1).unwrap())
        .unwrap()
        .unwrap();
    let query_vector: Bm25VectorOutput = query
        .get_by_index(NonZero::new(2).unwrap())
        .unwrap()
        .unwrap();
    let query_vector = query_vector.borrow();
    let target_vector = target_vector.borrow();

    let index =
        unsafe { pgrx::PgRelation::with_lock(index_oid, pgrx::pg_sys::AccessShareLock as _) };
    let meta = {
        let page = page_read(index.as_ptr(), METAPAGE_BLKNO);
        unsafe { &*(page.data().as_ptr() as *const MetaPageData) }
    };

    let term_stat_reader = TermStatReader::new(index.as_ptr(), meta.term_stat_blkno);
    let avgdl = meta.avgdl();
    let scores = bm25_score_batch(
        meta.doc_cnt,
        avgdl,
        &term_stat_reader,
        target_vector,
        query_vector,
    );

    scores * -1.0
}

#[pgrx::pg_extern()]
pub fn document_unicode_tokenize(text: &str, token_table: &str) -> Bm25VectorOutput {
    let tokens = unicode_tokenize(text);
    let args = Some(vec![(
        pgrx::PgBuiltInOids::TEXTARRAYOID.oid(),
        tokens.clone().into_datum(),
    )]);

    let mut token_ids = HashMap::new();
    pgrx::Spi::connect(|mut client| {
        let query = format!(
            r#"
            WITH new_tokens AS (SELECT unnest($1::text[]) AS token),
            to_insert AS (
                SELECT token FROM new_tokens
                WHERE NOT EXISTS (
                    SELECT 1 FROM bm25_catalog.{} WHERE token = new_tokens.token
                )
            ),
            ins AS (
                INSERT INTO bm25_catalog.{} (token)
                SELECT token FROM to_insert
                ON CONFLICT (token) DO NOTHING
                RETURNING id, token
            )
            SELECT id, token FROM ins
            UNION ALL
            SELECT id, token FROM bm25_catalog.{} WHERE token = ANY($1);
            "#,
            token_table, token_table, token_table
        );
        let table = client.update(&query, None, args).unwrap_or_report();
        for row in table {
            let id: i32 = row
                .get_by_name("id")
                .expect("no id column")
                .expect("no id value");
            let token: String = row
                .get_by_name("token")
                .expect("no token column")
                .expect("no token value");
            token_ids.insert(token, id as u32);
        }
    });

    let ids = tokens
        .iter()
        .map(|t| *token_ids.get(t).expect("unknown token"))
        .collect::<Vec<_>>();
    Bm25VectorOutput::from_ids(&ids)
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
pub fn query_unicode_tokenize(query: &str, token_table: &str) -> Bm25VectorOutput {
    let tokens = unicode_tokenize(query);
    let args = Some(vec![(
        pgrx::PgBuiltInOids::TEXTARRAYOID.oid(),
        tokens.clone().into_datum(),
    )]);
    let mut token_ids = HashMap::new();
    pgrx::Spi::connect(|client| {
        let table = client
            .select(
                &format!(
                    "SELECT id, token FROM bm25_catalog.{} WHERE token = ANY($1);",
                    token_table
                ),
                None,
                args,
            )
            .unwrap_or_report();
        for row in table {
            let id: i32 = row
                .get_by_name("id")
                .expect("no id column")
                .expect("no id value");
            let token: String = row
                .get_by_name("token")
                .expect("no token column")
                .expect("no token value");
            token_ids.insert(token, id as u32);
        }
    });

    let ids = tokens
        .iter()
        .filter(|&t| token_ids.contains_key(t))
        .map(|t| *token_ids.get(t).unwrap())
        .collect::<Vec<_>>();
    Bm25VectorOutput::from_ids(&ids)
}

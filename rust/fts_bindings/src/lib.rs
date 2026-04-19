// =========================================================================
// FTS Bindings — CXX Bridge Entry Point
// =========================================================================
//
// Defines the CXX bridge between ScyllaDB C++ and the Tantivy full-text
// search engine.  This is the only file compiled with `cxxbridge`; the
// implementation lives in the sub-modules.
//
// Relationship to the v1 (JSON-based) bridge:
//   - `FieldMapping` replaces the hardcoded 5-field schema.
//   - `FieldValue` replaces `payload_json: &str` in document ingestion.
//   - Search results carry only `(id, partition_key, score)` — no payload.
//   - `QueryParser` with correct default fields eliminates all v1 workarounds.

mod reader;
mod schema;
mod types;
mod writer;

#[cxx::bridge(namespace = "fts")]
mod ffi {
    // =========================================================================
    // Field mapping descriptor passed from C++ at index creation time.
    // Describes one CQL column → one Tantivy field.
    // =========================================================================
    struct FieldMapping {
        /// CQL column name (dotted path for UDT sub-fields, e.g.
        /// `address.city` for `address.city` within a UDT).
        name: String,
        /// Tantivy field kind: "text" | "string" | "i64" | "f64" | "bool" |
        /// "date" | "ip_addr" | "bytes" | "json"
        kind: String,
        /// Tokenizer name for text fields: "default" | "en_stem" |
        /// "keyword" | "whitespace" | "".
        /// Empty string selects the "default" (simple) tokenizer.
        tokenizer: String,
        /// `true` for set<T> / list<T> columns where multiple values are
        /// added per document using repeated `add_text` / `add_i64` calls.
        multi_valued: bool,
    }

    // =========================================================================
    // Typed field value passed from C++ for each column in a mutation.
    // Discriminated by `kind` to avoid boxing / dynamic dispatch across FFI.
    // =========================================================================
    struct FieldValue {
        /// Must match a `FieldMapping.name` in the index schema.
        field_name: String,
        /// "text" | "i64" | "f64" | "bool" | "date_us" | "ip" |
        /// "bytes" | "json" | "null"
        kind: String,
        /// Used for: text, string, ip (dotted notation), json (serialised),
        /// date (ISO-8601).  Also used for bytes (raw byte sequence as string).
        str_val: String,
        /// Used for: i64, bool (0 / 1), date_us (microseconds since epoch).
        i64_val: i64,
        /// Used for: f64.
        f64_val: f64,
    }

    // =========================================================================
    // Search result types
    // =========================================================================

    /// One matching row returned by a MATCH query.
    ///
    /// The C++ query path uses `partition_key` (and the clustering-key
    /// portion of `id`) to fetch the full row from the base table.  No
    /// stored payload is transferred across the FFI boundary.
    struct FtsSearchHit {
        /// "<partition_key>:<clustering_key>" or "<partition_key>" for
        /// tables without clustering columns.
        id: String,
        /// Serialised partition key — same format as `id`'s prefix.
        partition_key: String,
        /// BM25 relevance score assigned by Tantivy.
        score: f32,
    }

    /// One bucket in a facet aggregation result.
    struct FtsFacetBucket {
        value: String,
        count: u64,
    }

    /// Aggregated facet counts for a single field.
    struct FtsFacetResult {
        field: String,
        buckets: Vec<FtsFacetBucket>,
    }

    /// Complete response returned by `search()`.
    struct FtsSearchResponse {
        hits: Vec<FtsSearchHit>,
        total_hits: u64,
        /// Wall-clock time spent in Rust (microseconds).
        duration_us: u64,
        facets: Vec<FtsFacetResult>,
    }

    // =========================================================================
    // Opaque Rust types exported to C++
    // =========================================================================
    extern "Rust" {
        type ShardIndex;

        // ── Lifecycle ────────────────────────────────────────────────────

        /// Create a new shard index at `<path>/shard-<shard_id>/`.
        ///
        /// `field_mappings` is the descriptor built by `fts_index::validate()`
        /// from the CQL schema.  Must not be called twice for the same path.
        fn create_shard_index(
            path: &str,
            shard_id: u32,
            field_mappings: &[FieldMapping],
        ) -> Result<Box<ShardIndex>>;

        /// Open an existing shard index at `<path>/shard-<shard_id>/`.
        ///
        /// Used when ScyllaDB restarts with an existing FTS index on disk.
        fn open_shard_index(
            path: &str,
            shard_id: u32,
            field_mappings: &[FieldMapping],
        ) -> Result<Box<ShardIndex>>;

        // ── Write operations (called from alien thread) ──────────────────

        /// Insert or update a document.  Implements last-writer-wins: if a
        /// document with an equal or newer `writetime_us` already exists the
        /// call is a no-op.
        ///
        /// `expires_at_us` of `i64::MAX` means no TTL.
        fn upsert_document(
            index: &mut ShardIndex,
            doc_id: &str,
            partition_key: &str,
            fields: &[FieldValue],
            writetime_us: u64,
            expires_at_us: i64,
        ) -> Result<()>;

        /// Delete the document with the given `doc_id`.
        fn delete_document(index: &mut ShardIndex, doc_id: &str) -> Result<()>;

        /// Delete all documents belonging to `partition_key`.
        fn delete_by_partition_key(index: &mut ShardIndex, partition_key: &str) -> Result<()>;

        /// Flush staged writes and make them visible to readers.
        /// Returns the total document count after the commit.
        fn commit(index: &mut ShardIndex) -> Result<u64>;

        /// Delete all expired documents (TTL pruning).
        /// Returns the number of documents removed.
        fn prune_expired(index: &mut ShardIndex) -> Result<u64>;

        // ── Read operations (called from alien thread) ───────────────────

        /// Execute a full-text MATCH query.
        ///
        /// `default_field` is the CQL column name to use as the sole default
        /// field for the Tantivy `QueryParser`.  An empty string falls back to
        /// all TEXT-kind user fields (same behaviour as before this parameter
        /// was added — useful for multi-field bare searches).
        fn search(
            index: &ShardIndex,
            query: &str,
            default_field: &str,
            limit: u32,
            offset: u32,
            facet_fields: &[String],
            group_by_partition: bool,
        ) -> Result<Box<FtsSearchResponse>>;

        /// Return all document IDs for the given partition key.
        fn list_ids_by_partition_key(
            index: &ShardIndex,
            partition_key: &str,
        ) -> Result<Vec<String>>;

        // ── Maintenance ──────────────────────────────────────────────────

        /// Total number of documents currently in the index.
        fn doc_count(index: &ShardIndex) -> Result<u64>;

        /// Release all resources and delete segment files.
        /// The caller is responsible for removing the shard directory.
        fn drop_index(index: &mut ShardIndex) -> Result<()>;
    }
}

// ── Bridge function implementations ──────────────────────────────────────────
//
// These thin wrappers delegate to the sub-modules that contain the
// actual implementation.

use crate::types::ShardIndex;

fn create_shard_index(
    path: &str,
    shard_id: u32,
    field_mappings: &[ffi::FieldMapping],
) -> anyhow::Result<Box<ShardIndex>> {
    schema::build_shard_index(path, shard_id, field_mappings).map(Box::new)
}

fn open_shard_index(
    path: &str,
    shard_id: u32,
    field_mappings: &[ffi::FieldMapping],
) -> anyhow::Result<Box<ShardIndex>> {
    schema::open_shard_index(path, shard_id, field_mappings).map(Box::new)
}

fn upsert_document(
    index: &mut ShardIndex,
    doc_id: &str,
    partition_key: &str,
    fields: &[ffi::FieldValue],
    writetime_us: u64,
    expires_at_us: i64,
) -> anyhow::Result<()> {
    writer::upsert_document(
        index,
        doc_id,
        partition_key,
        fields,
        writetime_us,
        expires_at_us,
    )
}

fn delete_document(index: &mut ShardIndex, doc_id: &str) -> anyhow::Result<()> {
    writer::delete_document(index, doc_id)
}

fn delete_by_partition_key(index: &mut ShardIndex, partition_key: &str) -> anyhow::Result<()> {
    writer::delete_by_partition_key(index, partition_key)
}

fn commit(index: &mut ShardIndex) -> anyhow::Result<u64> {
    writer::commit(index)
}

fn prune_expired(index: &mut ShardIndex) -> anyhow::Result<u64> {
    writer::prune_expired(index)
}

fn search(
    index: &ShardIndex,
    query: &str,
    default_field: &str,
    limit: u32,
    offset: u32,
    facet_fields: &[String],
    group_by_partition: bool,
) -> anyhow::Result<Box<ffi::FtsSearchResponse>> {
    reader::search(
        index,
        query,
        default_field,
        limit,
        offset,
        facet_fields,
        group_by_partition,
    )
}

fn list_ids_by_partition_key(
    index: &ShardIndex,
    partition_key: &str,
) -> anyhow::Result<Vec<String>> {
    reader::list_ids_by_partition_key(index, partition_key)
}

fn doc_count(index: &ShardIndex) -> anyhow::Result<u64> {
    reader::doc_count(index)
}

fn drop_index(index: &mut ShardIndex) -> anyhow::Result<()> {
    writer::drop_index(index)
}

// =========================================================================
// Unit / integration tests
// =========================================================================
//
// These tests exercise the Rust internals (schema construction, write, read)
// without crossing the CXX FFI boundary.  They require only `cargo test`.
//
// Run with:
//   cargo test --manifest-path scylladb/rust/fts_bindings/Cargo.toml

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::{FieldMapping, FieldValue};

    // ── Helpers ───────────────────────────────────────────────────────────

    fn text_mapping(name: &str) -> FieldMapping {
        FieldMapping {
            name: name.to_string(),
            kind: "text".to_string(),
            tokenizer: "default".to_string(),
            multi_valued: false,
        }
    }

    fn i64_mapping(name: &str) -> FieldMapping {
        FieldMapping {
            name: name.to_string(),
            kind: "i64".to_string(),
            tokenizer: String::new(),
            multi_valued: false,
        }
    }

    fn text_val(field: &str, val: &str) -> FieldValue {
        FieldValue {
            field_name: field.to_string(),
            kind: "text".to_string(),
            str_val: val.to_string(),
            i64_val: 0,
            f64_val: 0.0,
        }
    }

    fn i64_val(field: &str, val: i64) -> FieldValue {
        FieldValue {
            field_name: field.to_string(),
            kind: "i64".to_string(),
            str_val: String::new(),
            i64_val: val,
            f64_val: 0.0,
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────

    /// Basic round-trip: create → insert → commit → search.
    #[test]
    fn test_create_insert_search() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();

        let mappings = vec![text_mapping("body")];
        let mut idx =
            schema::build_shard_index(path, 0, &mappings).expect("build_shard_index must succeed");

        writer::upsert_document(
            &mut idx,
            "pk1:ck1",
            "pk1",
            &[text_val("body", "hello world")],
            1_000,
            i64::MAX,
        )
        .expect("upsert_document must succeed");

        let n = writer::commit(&mut idx).expect("commit must succeed");
        assert_eq!(n, 1, "one document after first commit");

        let resp = reader::search(&idx, "hello", "body", 10, 0, &[], false).expect("search must succeed");
        assert_eq!(resp.hits.len(), 1, "exactly one hit expected");
        assert_eq!(resp.hits[0].id, "pk1:ck1");
        assert_eq!(resp.hits[0].partition_key, "pk1");
        assert!(resp.hits[0].score > 0.0, "BM25 score must be positive");
        assert_eq!(resp.total_hits, 1);
    }

    /// Pagination via `offset` parameter.
    #[test]
    fn test_pagination() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();

        let mappings = vec![text_mapping("body")];
        let mut idx = schema::build_shard_index(path, 0, &mappings).unwrap();

        for i in 0..5u32 {
            writer::upsert_document(
                &mut idx,
                &format!("pk:ck{}", i),
                "pk",
                &[text_val("body", "common text everywhere")],
                i as u64 + 1,
                i64::MAX,
            )
            .unwrap();
        }
        writer::commit(&mut idx).unwrap();

        let page0 = reader::search(&idx, "common", "body", 3, 0, &[], false).unwrap();
        let page1 = reader::search(&idx, "common", "body", 3, 3, &[], false).unwrap();

        assert_eq!(page0.hits.len(), 3, "page 0 must have 3 hits");
        assert_eq!(page1.hits.len(), 2, "page 1 must have remaining 2 hits");
        assert_eq!(
            page0.total_hits, 5,
            "total_hits must reflect full result set"
        );
    }

    /// Last-writer-wins: an older writetime must not overwrite a newer one.
    #[test]
    fn test_last_writer_wins() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();

        let mappings = vec![text_mapping("body")];
        let mut idx = schema::build_shard_index(path, 0, &mappings).unwrap();

        // Insert the "new" version first (higher writetime).
        writer::upsert_document(
            &mut idx,
            "pk:ck",
            "pk",
            &[text_val("body", "new content")],
            2_000,
            i64::MAX,
        )
        .unwrap();

        // Attempt to overwrite with an older writetime — must be a no-op.
        writer::upsert_document(
            &mut idx,
            "pk:ck",
            "pk",
            &[text_val("body", "old content")],
            1_000,
            i64::MAX,
        )
        .unwrap();

        writer::commit(&mut idx).unwrap();

        let r_new = reader::search(&idx, "new", "body", 10, 0, &[], false).unwrap();
        let r_old = reader::search(&idx, "old", "body", 10, 0, &[], false).unwrap();

        assert_eq!(r_new.total_hits, 1, "new version must be present");
        assert_eq!(r_old.total_hits, 0, "old version must not overwrite");
    }

    /// Delete a single document by doc_id.
    #[test]
    fn test_delete_document() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();

        let mappings = vec![text_mapping("body")];
        let mut idx = schema::build_shard_index(path, 0, &mappings).unwrap();

        writer::upsert_document(
            &mut idx,
            "pk:ck",
            "pk",
            &[text_val("body", "to be deleted")],
            1_000,
            i64::MAX,
        )
        .unwrap();
        writer::commit(&mut idx).unwrap();

        writer::delete_document(&mut idx, "pk:ck").unwrap();
        writer::commit(&mut idx).unwrap();

        let n = reader::doc_count(&idx).unwrap();
        assert_eq!(n, 0, "index must be empty after delete+commit");
    }

    /// Delete all documents belonging to a partition key.
    #[test]
    fn test_delete_by_partition_key() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();

        let mappings = vec![text_mapping("body")];
        let mut idx = schema::build_shard_index(path, 0, &mappings).unwrap();

        // Insert two docs for "pk_a" and one for "pk_b".
        for ck in ["ck1", "ck2"] {
            writer::upsert_document(
                &mut idx,
                &format!("pk_a:{}", ck),
                "pk_a",
                &[text_val("body", "content a")],
                1_000,
                i64::MAX,
            )
            .unwrap();
        }
        writer::upsert_document(
            &mut idx,
            "pk_b:ck1",
            "pk_b",
            &[text_val("body", "content b")],
            1_000,
            i64::MAX,
        )
        .unwrap();
        writer::commit(&mut idx).unwrap();

        writer::delete_by_partition_key(&mut idx, "pk_a").unwrap();
        writer::commit(&mut idx).unwrap();

        let n = reader::doc_count(&idx).unwrap();
        assert_eq!(n, 1, "only pk_b document must remain");

        let ids = reader::list_ids_by_partition_key(&idx, "pk_b").unwrap();
        assert_eq!(ids, vec!["pk_b:ck1"]);
    }

    /// Reopen an existing on-disk index and verify data survives.
    #[test]
    fn test_open_existing_index() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();
        let mappings = vec![text_mapping("body"), i64_mapping("count")];

        // Phase 1: create and populate, then drop (simulates a restart).
        {
            let mut idx = schema::build_shard_index(path, 0, &mappings).unwrap();
            writer::upsert_document(
                &mut idx,
                "pk:ck",
                "pk",
                &[text_val("body", "persistent data"), i64_val("count", 42)],
                1_000,
                i64::MAX,
            )
            .unwrap();
            writer::commit(&mut idx).unwrap();
        }

        // Phase 2: reopen and verify.
        {
            let idx = schema::open_shard_index(path, 0, &mappings).unwrap();
            let resp = reader::search(&idx, "persistent", "body", 10, 0, &[], false).unwrap();
            assert_eq!(resp.hits.len(), 1);
            assert_eq!(resp.hits[0].id, "pk:ck");
        }
    }

    /// Unknown field names in FieldValue must be silently ignored (column
    /// added after CREATE INDEX — will be visible after a full rebuild).
    #[test]
    fn test_unknown_field_silently_ignored() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();

        let mappings = vec![text_mapping("body")];
        let mut idx = schema::build_shard_index(path, 0, &mappings).unwrap();

        let fields = vec![
            text_val("body", "known field"),
            text_val("extra", "unknown — must be silently ignored"),
        ];
        let result = writer::upsert_document(&mut idx, "pk:ck", "pk", &fields, 1_000, i64::MAX);
        assert!(result.is_ok(), "upsert with unknown field must not fail");

        writer::commit(&mut idx).unwrap();
        assert_eq!(reader::doc_count(&idx).unwrap(), 1);
    }

    /// An invalid field kind in FieldMapping must be rejected at schema
    /// construction time with a clear error.
    #[test]
    fn test_invalid_field_kind_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();

        let mappings = vec![FieldMapping {
            name: "bad".to_string(),
            kind: "not_a_real_type".to_string(),
            tokenizer: String::new(),
            multi_valued: false,
        }];
        let result = schema::build_shard_index(path, 0, &mappings);
        assert!(result.is_err(), "invalid field kind must be rejected");
    }

    /// Boolean OR expression must parse and return both matching documents.
    ///
    /// This is the regression test for the root cause: previously the C++ side
    /// wrapped the query as `field:(wonder OR builder)` which Tantivy's parser
    /// rejects.  With `default_field` the raw expression is passed directly.
    #[test]
    fn test_boolean_or_query() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();

        let mappings = vec![text_mapping("username")];
        let mut idx = schema::build_shard_index(path, 0, &mappings).unwrap();

        writer::upsert_document(
            &mut idx,
            "pk:ck1",
            "pk",
            &[text_val("username", "wonderland")],
            1_000,
            i64::MAX,
        )
        .unwrap();
        writer::upsert_document(
            &mut idx,
            "pk:ck2",
            "pk",
            &[text_val("username", "builder")],
            2_000,
            i64::MAX,
        )
        .unwrap();
        writer::upsert_document(
            &mut idx,
            "pk:ck3",
            "pk",
            &[text_val("username", "something_else")],
            3_000,
            i64::MAX,
        )
        .unwrap();
        writer::commit(&mut idx).unwrap();

        let resp = reader::search(&idx, "wonderland OR builder", "username", 10, 0, &[], false)
            .expect("boolean OR query must not fail");
        assert_eq!(resp.total_hits, 2, "both wonderland and builder must match");
    }
}

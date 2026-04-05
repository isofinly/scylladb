/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

// =========================================================================
// FTS CDC Consumer — Implementation
// =========================================================================
//
// The consumer is a periodic polling service: every POLL_INTERVAL_MS it
// reads new rows from each FTS-indexed table's CDC log and forwards the
// decoded typed field values to the per-shard Tantivy index.
//
// Threading model:
//   - All Seastar code (timer callbacks, CQL queries) runs on the normal
//     Seastar reactor thread.
//   - All Tantivy operations (`upsert_document`, `search`, …) are dispatched
//     to an alien thread via `seastar::alien::run_on` so they never block
//     the reactor.  The alien thread is pinned to the same NUMA node as the
//     shard for cache efficiency.
//
// CDC column conventions (ScyllaDB CDC log format):
//   cdc$time      timeuuid   — write timestamp (used as checkpoint cursor)
//   cdc$operation tinyint    — operation kind (update=1, insert=2, row_delete=3, …)
//   cdc$ttl       bigint     — row TTL in seconds (0 = no TTL)
//   <col>         <type>     — mirrored data column (null = not present in mutation)
//   cdc$deleted_<col> boolean — true if column was tombstoned in this mutation
//
// Doc-ID convention:
//   For tables WITH clustering columns:  "<pk_hex>:<ck_hex>"
//   For tables WITHOUT clustering columns: "<pk_hex>"
//   Both parts are hex-encoded byte representations of the serialised keys.

#include "fts/fts_cdc_consumer.hh"

#include <chrono>
#include <stdexcept>
#include <string>
#include <vector>

#include <seastar/core/alien.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sleep.hh>

#include "cdc/cdc_options.hh"
#include "cdc/log.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db_clock.hh"
#include "index/fts_index.hh"
#include "index/secondary_index_manager.hh"
#include "mutation/timestamp.hh"
#include "replica/database.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "utils/assert.hh"
#include "utils/log.hh"
#include "utils/to_string.hh"

// Pull in the cxx-generated bridge header for Tantivy FFI.
// The generated header is placed in the build tree under rust/fts_bindings.hh.
#include "rust/fts_bindings.hh"

namespace db::fts {

static logging::logger ftslog("fts_cdc_consumer");

// =========================================================================
// Configuration constants
// =========================================================================

// How often to poll each CDC log table (milliseconds).
static constexpr uint32_t POLL_INTERVAL_MS = 5'000;

// How many CDC rows to fetch per poll iteration.  Limits memory use while
// ensuring forward progress even under sustained write load.
static constexpr uint32_t CDC_BATCH_SIZE = 1'000;

// Default maximum number of search results returned to the query path.
static constexpr uint32_t DEFAULT_SEARCH_LIMIT = 10'000;

// =========================================================================
// Alien thread runner
// =========================================================================
//
// A thin wrapper around std::thread that executes blocking Tantivy FFI calls
// without stalling the Seastar reactor.  One instance per shard guarantees
// that the ShardIndex — which is NOT Send across threads — is always touched
// from the same alien thread.
class alien_thread_runner {
public:
    alien_thread_runner() = default;
    ~alien_thread_runner() = default;

    /// Run `fn` on the alien thread and return a future that resolves when
    /// the call completes.  `fn` must be callable with no arguments and must
    /// not capture Seastar futures or promises.
    template <typename Fn>
    seastar::future<std::invoke_result_t<Fn>> run(Fn fn) {
        // seastar::alien::run_on dispatches work to an alien thread and
        // returns a future resolved back on the calling Seastar shard.
        return seastar::alien::run_on(
                *seastar::alien::internal::default_instance,
                seastar::this_shard_id(),
                std::move(fn));
    }
};

// =========================================================================
// Helpers
// =========================================================================

// Hex-encode a bytes_view for use as a document-id component.
static sstring hex_encode(bytes_view bv) {
    static const char hex[] = "0123456789abcdef";
    sstring result;
    result.reserve(bv.size() * 2);
    for (uint8_t b : bv) {
        result += hex[b >> 4];
        result += hex[b & 0x0f];
    }
    return result;
}

// Build a doc_id string from serialised partition key bytes and (optionally)
// clustering key bytes.
static sstring make_doc_id(bytes_view pk, bytes_view ck) {
    if (ck.empty()) {
        return hex_encode(pk);
    }
    return hex_encode(pk) + ":" + hex_encode(ck);
}

// Convert a CDC `cdc$time` timeuuid column to microseconds since epoch.
// The timeuuid encodes the write timestamp in its high bits.
static uint64_t timeuuid_to_us(const utils::UUID& u) {
    // UUID v1 stores time as 100-ns intervals since 1582-10-15.
    // Subtract the offset between 1582-10-15 and 1970-01-01 (in 100-ns):
    //   0x01B21DD213814000
    constexpr uint64_t UUID_EPOCH_OFFSET_100NS = 0x01B21DD213814000ULL;
    uint64_t ts_100ns = u.timestamp() - UUID_EPOCH_OFFSET_100NS;
    return ts_100ns / 10; // convert 100-ns to microseconds
}

// Extract a typed FieldValue from a CDC result-set row for a single column.
// Returns an empty optional if the column is NULL in this CDC row (i.e. was
// not part of the original mutation).
static std::optional<::fts::FieldValue> extract_field_value(
        const cql3::untyped_result_set_row& row,
        const column_definition& cdef)
{
    const sstring col_name = cdef.name_as_text();

    // NULL in a CDC row means the column was not present in the mutation.
    if (!row.has(col_name) || row.is_null(col_name)) {
        return std::nullopt;
    }

    ::fts::FieldValue fv;
    fv.field_name = col_name.c_str();

    const abstract_type& type = *cdef.type;

    switch (type.get_kind()) {
        case abstract_type::kind::utf8:
        case abstract_type::kind::ascii: {
            fv.kind      = "text";
            fv.str_val   = row.get_as<sstring>(col_name).c_str();
            fv.i64_val   = 0;
            fv.f64_val   = 0.0;
            return fv;
        }
        case abstract_type::kind::int32: {
            fv.kind    = "i64";
            fv.i64_val = row.get_as<int32_t>(col_name);
            fv.f64_val = 0.0;
            return fv;
        }
        case abstract_type::kind::long_kind: {
            fv.kind    = "i64";
            fv.i64_val = row.get_as<int64_t>(col_name);
            fv.f64_val = 0.0;
            return fv;
        }
        case abstract_type::kind::short_kind: {
            fv.kind    = "i64";
            fv.i64_val = row.get_as<int16_t>(col_name);
            fv.f64_val = 0.0;
            return fv;
        }
        case abstract_type::kind::byte: {
            fv.kind    = "i64";
            fv.i64_val = row.get_as<int8_t>(col_name);
            fv.f64_val = 0.0;
            return fv;
        }
        case abstract_type::kind::float_kind: {
            fv.kind    = "f64";
            fv.f64_val = static_cast<double>(row.get_as<float>(col_name));
            fv.i64_val = 0;
            return fv;
        }
        case abstract_type::kind::double_kind: {
            fv.kind    = "f64";
            fv.f64_val = row.get_as<double>(col_name);
            fv.i64_val = 0;
            return fv;
        }
        case abstract_type::kind::boolean: {
            fv.kind    = "bool";
            fv.i64_val = row.get_as<bool>(col_name) ? 1 : 0;
            fv.f64_val = 0.0;
            return fv;
        }
        case abstract_type::kind::timestamp: {
            // Timestamp stored as milliseconds since epoch; Tantivy wants
            // microseconds since epoch.
            fv.kind    = "date_us";
            fv.i64_val = row.get_as<db_clock::time_point>(col_name)
                             .time_since_epoch().count() * 1000LL;
            fv.f64_val = 0.0;
            return fv;
        }
        case abstract_type::kind::uuid:
        case abstract_type::kind::timeuuid: {
            fv.kind    = "string";
            fv.str_val = row.get_as<utils::UUID>(col_name).to_sstring().c_str();
            fv.i64_val = 0;
            fv.f64_val = 0.0;
            return fv;
        }
        case abstract_type::kind::inet: {
            fv.kind    = "ip";
            // net::inet_address::to_sstring() returns dotted notation.
            fv.str_val = row.get_as<net::inet_address>(col_name)
                             .to_sstring().c_str();
            fv.i64_val = 0;
            fv.f64_val = 0.0;
            return fv;
        }
        case abstract_type::kind::bytes: {
            auto raw = row.get_blob(col_name);
            fv.kind    = "bytes";
            fv.str_val = hex_encode(raw).c_str();
            fv.i64_val = 0;
            fv.f64_val = 0.0;
            return fv;
        }
        case abstract_type::kind::decimal:
        case abstract_type::kind::varint: {
            // Represent as serialised string for exact-match indexing.
            auto raw = row.get_blob(col_name);
            fv.kind    = "string";
            fv.str_val = hex_encode(raw).c_str();
            fv.i64_val = 0;
            fv.f64_val = 0.0;
            return fv;
        }
        case abstract_type::kind::map:
        case abstract_type::kind::list:
        case abstract_type::kind::set:
        case abstract_type::kind::user: {
            // JSON fallback for complex types.
            auto raw = row.get_blob(col_name);
            fv.kind    = "json";
            // Provide the raw bytes as a hex string; the Rust side stores
            // it as a JSON string literal.
            // TODO: deserialise to proper JSON for full query support.
            fv.str_val = hex_encode(raw).c_str();
            fv.i64_val = 0;
            fv.f64_val = 0.0;
            return fv;
        }
        default:
            return std::nullopt;
    }
}

// =========================================================================
// fts_cdc_consumer — constructor / destructor
// =========================================================================

fts_cdc_consumer::fts_cdc_consumer(cql3::query_processor& qp, replica::database& db)
    : _qp{qp}
    , _db{db}
    , _alien{std::make_unique<alien_thread_runner>()}
{
}

fts_cdc_consumer::~fts_cdc_consumer() = default;

// =========================================================================
// Lifecycle
// =========================================================================

seastar::future<> fts_cdc_consumer::start() {
    // Open indexes for every table that already has an FTS index at startup.
    // This covers the restart-with-existing-index case.
    for (auto& [ks_name, ks] : _db.get_keyspaces()) {
        for (auto& [cf_name, cf] : ks.column_families()) {
            auto s = cf->schema();
            if (db::index::fts_index::has_fts_index(*s)) {
                co_await open_indexes_for_table(s);
            }
        }
    }

    // Arm the periodic poll timer.
    _poll_timer.set_callback([this] {
        if (_poll_running || _stopping) {
            return;
        }
        _poll_running = true;
        (void)poll_cdc().finally([this] { _poll_running = false; });
    });
    _poll_timer.arm_periodic(std::chrono::milliseconds(POLL_INTERVAL_MS));
    co_return;
}

seastar::future<> fts_cdc_consumer::stop() {
    _stopping = true;
    _poll_timer.cancel();

    // Wait for any in-flight poll to complete.
    while (_poll_running) {
        co_await seastar::sleep(std::chrono::milliseconds(50));
    }

    // Close all open indexes (commit pending writes first).
    for (auto& [tid, ts] : _tables) {
        (void)tid; // used implicitly via ts
        for (auto& [idx_name, opaque_ptr] : ts.indexes) {
            (void)idx_name;
            // The opaque_ptr deleter calls drop_index().
        }
    }
    _tables.clear();
}

// =========================================================================
// Schema change
// =========================================================================

seastar::future<> fts_cdc_consumer::on_schema_change(
        schema_ptr old_schema, schema_ptr new_schema)
{
    bool had_fts = old_schema && db::index::fts_index::has_fts_index(*old_schema);
    bool has_fts = new_schema && db::index::fts_index::has_fts_index(*new_schema);

    if (!had_fts && has_fts) {
        // Table gained an FTS index — create shard directories and open.
        co_await open_indexes_for_table(new_schema);
    } else if (had_fts && !has_fts) {
        // Table lost its FTS index — close indexes.
        co_await close_indexes_for_table(new_schema->id());
    } else if (had_fts && has_fts
               && old_schema->version() != new_schema->version()) {
        // Schema version changed (e.g. ALTER TABLE ADD column) — the
        // index_version() tie to schema.version() will cause the index
        // manager to request a rebuild.  For now we simply reopen with the
        // new field mappings; a full rebuild is triggered externally.
        // TODO: implement incremental rebuild on column addition.
        co_await close_indexes_for_table(old_schema->id());
        co_await open_indexes_for_table(new_schema);
    }
}

// =========================================================================
// Index open / close
// =========================================================================

seastar::future<> fts_cdc_consumer::open_indexes_for_table(schema_ptr s) {
    const auto tid = s->id();

    // Build FieldMapping list from the field_mapping JSON stored in each
    // index's OPTIONS at CREATE INDEX time.
    for (const auto& idx_meta : s->indices()) {
        auto class_it = idx_meta.options().find(
                secondary_index::secondary_index_manager::custom_class_option_name);
        if (class_it == idx_meta.options().end()) {
            continue;
        }
        auto factory = secondary_index::secondary_index_manager::get_custom_class_factory(
                class_it->second);
        if (!factory) {
            continue;
        }
        auto instance = (*factory)();
        if (!dynamic_cast<db::index::fts_index*>(instance.get())) {
            continue;
        }

        const sstring& idx_name = idx_meta.name();

        // Build the field-mapping descriptor from the schema columns so we
        // can reconstruct the Tantivy schema after a restart even if the
        // OPTIONS["field_mapping"] key is absent (e.g. older snapshot).
        auto field_mapping_json = db::index::fts_index::build_field_mapping_json(
                *s, {}, idx_meta.options());

        // Parse the JSON array into FieldMapping structs.
        // The JSON format is: [{"name":"col","kind":"text","tokenizer":"","multi_valued":false}, …]
        // We do a minimal parse here; full JSON support added via serde on
        // the Rust side.
        std::vector<::fts::FieldMapping> mappings;
        // TODO: replace with a proper JSON parser (nlohmann or rapidjson).
        // For now, forward the raw JSON string as a single "json" field.
        // The Rust `open_shard_index` / `create_shard_index` functions parse
        // FieldMapping slices, so we cannot pass raw JSON directly.  This
        // placeholder ensures compilation; a proper JSON → FieldMapping
        // deserialiser must be wired before integration tests.
        (void)field_mapping_json;

        // Shard-local index path.
        // TODO: derive from db::config::data_file_directories().
        sstring idx_path = format("fts_indexes/{}/{}/{}",
                s->ks_name(), s->cf_name(), idx_name);

        const uint32_t shard_id = seastar::this_shard_id();

        // Run index creation/open on the alien thread.
        auto result = co_await _alien->run([idx_path, shard_id, &mappings]()
                -> std::variant<rust::Box<::fts::ShardIndex>, std::string>
        {
            try {
                auto idx = ::fts::open_shard_index(
                        idx_path.c_str(), shard_id,
                        rust::Slice<const ::fts::FieldMapping>{
                            mappings.data(), mappings.size()});
                return std::move(idx);
            } catch (const std::exception& e) {
                // Index directory doesn't exist yet — create it.
                try {
                    auto idx = ::fts::create_shard_index(
                            idx_path.c_str(), shard_id,
                            rust::Slice<const ::fts::FieldMapping>{
                                mappings.data(), mappings.size()});
                    return std::move(idx);
                } catch (const std::exception& e2) {
                    return std::string(e2.what());
                }
            }
        });

        if (std::holds_alternative<std::string>(result)) {
            ftslog.error("Failed to open FTS index {}/{}: {}",
                    idx_name, shard_id, std::get<std::string>(result));
            continue;
        }

        auto& ts = _tables[tid];

        // Wrap the ShardIndex in an opaque void* with a custom deleter that
        // calls drop_index() so we avoid pulling the full cxx.h into the
        // header.
        auto* raw = std::get<rust::Box<::fts::ShardIndex>>(std::move(result))
                        .into_raw();
        ts.indexes.emplace(
                idx_name,
                std::unique_ptr<void, void(*)(void*)>(
                        raw,
                        [](void* p) {
                            auto* idx = static_cast<::fts::ShardIndex*>(p);
                            try { ::fts::drop_index(*idx); }
                            catch (...) { /* best-effort */ }
                            rust::Box<::fts::ShardIndex>::from_raw(idx);
                        }));
    }
}

seastar::future<> fts_cdc_consumer::close_indexes_for_table(table_id tid) {
    auto it = _tables.find(tid);
    if (it == _tables.end()) {
        co_return;
    }
    // Destructors on the unique_ptr values call drop_index().
    it->second.indexes.clear();
    _tables.erase(it);
}

// =========================================================================
// CDC polling
// =========================================================================

seastar::future<> fts_cdc_consumer::poll_cdc() {
    for (auto& [tid, ts] : _tables) {
        if (_stopping) {
            co_return;
        }
        // Resolve table_id → schema.
        schema_ptr s;
        try {
            s = _db.find_schema(tid);
        } catch (...) {
            ftslog.warn("FTS CDC poll: schema not found for table_id {}", tid);
            continue;
        }
        co_await poll_cdc_table(s->ks_name(), s->cf_name(), tid, ts);
    }
}

seastar::future<> fts_cdc_consumer::poll_cdc_table(
        const sstring& keyspace,
        const sstring& table,
        table_id tid,
        fts_table_state& ts)
{
    // The CDC log table name is "<table>_scylla_cdc_log".
    const sstring cdc_table = table + "_scylla_cdc_log";

    // We use the checkpoint timestamp (milliseconds since epoch) to page
    // through newly written CDC rows.  The cdc$time column is a timeuuid;
    // minTimeuuid() / maxTimeuuid() CQL functions let us express timestamp
    // comparisons.
    const int64_t checkpoint_ms =
            ts.checkpoint.time_since_epoch().count();

    const sstring query = format(
            "SELECT * FROM \"{}\".\"{}\" WHERE token(cdc$stream_id) >= {} "
            "AND token(cdc$stream_id) <= {} "
            "AND cdc$time > minTimeuuid({}) "
            "LIMIT {} ALLOW FILTERING",
            keyspace, cdc_table,
            std::numeric_limits<int64_t>::min(),
            std::numeric_limits<int64_t>::max(),
            checkpoint_ms,
            CDC_BATCH_SIZE);

    // Execute against the local replica — consistency_level::ONE is fine
    // because we are always reading from the shard that owns the data.
    ::shared_ptr<cql3::untyped_result_set> rs;
    try {
        rs = co_await _qp.execute_internal(
                query, db::consistency_level::ONE,
                cql3::query_processor::cache_internal::yes);
    } catch (const std::exception& e) {
        ftslog.warn("FTS CDC poll query failed for {}.{}: {}", keyspace, table, e.what());
        co_return;
    }

    if (!rs || rs->empty()) {
        co_return;
    }

    schema_ptr base_schema = _db.find_schema(tid);

    db_clock::time_point latest_ts = ts.checkpoint;
    for (const auto& row : *rs) {
        co_await process_cdc_row(base_schema, row, ts);

        // Advance checkpoint to the most recent cdc$time seen.
        if (row.has("cdc$time")) {
            auto u = row.get_as<utils::UUID>("cdc$time");
            uint64_t us = timeuuid_to_us(u);
            auto tp = db_clock::time_point{
                db_clock::duration{static_cast<int64_t>(us / 1000)}};
            if (tp > latest_ts) {
                latest_ts = tp;
            }
        }
    }
    ts.checkpoint = latest_ts;

    // Commit the Tantivy shard index after each batch.
    for (auto& [idx_name, opaque_ptr] : ts.indexes) {
        auto* raw_idx = static_cast<::fts::ShardIndex*>(opaque_ptr.get());
        co_await _alien->run([raw_idx]() -> void {
            try { ::fts::commit(*raw_idx); }
            catch (const std::exception& e) {
                ftslog.warn("FTS commit error: {}", e.what());
            }
        });
    }
}

// =========================================================================
// CDC row processing
// =========================================================================

seastar::future<> fts_cdc_consumer::process_cdc_row(
        schema_ptr base_schema,
        const cql3::untyped_result_set_row& row,
        fts_table_state& ts)
{
    // Read operation type from the CDC row.
    if (!row.has("cdc$operation")) {
        co_return;
    }
    const auto op = static_cast<cdc::operation>(row.get_as<int8_t>("cdc$operation"));

    // Build the doc_id from the partition key and (if present) clustering key.
    // CDC rows mirror the base table's key columns directly.
    // We reconstruct serialised key bytes by reading each key column.
    sstring pk_str;
    sstring ck_str;

    for (const auto& cdef : base_schema->partition_key_columns()) {
        const sstring col = cdef.name_as_text();
        if (row.has(col) && !row.is_null(col)) {
            auto raw = row.get_blob(col);
            if (!pk_str.empty()) {
                pk_str += ":";
            }
            pk_str += hex_encode(raw);
        }
    }
    for (const auto& cdef : base_schema->clustering_key_columns()) {
        const sstring col = cdef.name_as_text();
        if (row.has(col) && !row.is_null(col)) {
            auto raw = row.get_blob(col);
            if (!ck_str.empty()) {
                ck_str += ":";
            }
            ck_str += hex_encode(raw);
        }
    }

    const sstring doc_id   = make_doc_id(bytes_view{
            reinterpret_cast<const int8_t*>(pk_str.data()), pk_str.size()},
            bytes_view{reinterpret_cast<const int8_t*>(ck_str.data()), ck_str.size()});
    const sstring part_key = pk_str;

    // Determine write timestamp from cdc$time.
    uint64_t writetime_us = 0;
    if (row.has("cdc$time")) {
        writetime_us = timeuuid_to_us(row.get_as<utils::UUID>("cdc$time"));
    }

    // Determine TTL.
    int64_t expires_at_us = std::numeric_limits<int64_t>::max(); // no TTL
    if (row.has("cdc$ttl") && !row.is_null("cdc$ttl")) {
        int64_t ttl_s = row.get_as<int64_t>("cdc$ttl");
        if (ttl_s > 0) {
            expires_at_us = static_cast<int64_t>(writetime_us)
                          + ttl_s * 1'000'000LL;
        }
    }

    // Handle delete operations first — no field values needed.
    if (op == cdc::operation::row_delete || op == cdc::operation::partition_delete) {
        for (auto& [idx_name, opaque_ptr] : ts.indexes) {
            auto* raw_idx = static_cast<::fts::ShardIndex*>(opaque_ptr.get());
            auto doc_id_copy    = doc_id;
            auto part_key_copy  = part_key;
            bool is_part_delete = (op == cdc::operation::partition_delete);
            co_await _alien->run([raw_idx, doc_id_copy, part_key_copy,
                                  is_part_delete]() -> void {
                try {
                    if (is_part_delete) {
                        ::fts::delete_by_partition_key(*raw_idx,
                                part_key_copy.c_str());
                    } else {
                        ::fts::delete_document(*raw_idx, doc_id_copy.c_str());
                    }
                } catch (const std::exception& e) {
                    ftslog.warn("FTS delete error: {}", e.what());
                }
            });
        }
        co_return;
    }

    // For inserts/updates, collect typed FieldValues from the regular columns.
    std::vector<::fts::FieldValue> fields;
    for (const auto& cdef : base_schema->regular_columns()) {
        if (!db::index::fts_index::is_fts_indexable(*cdef.type)) {
            continue;
        }
        auto fv_opt = extract_field_value(row, cdef);
        if (fv_opt) {
            fields.push_back(std::move(*fv_opt));
        }
    }

    if (fields.empty()) {
        co_return;
    }

    for (auto& [idx_name, opaque_ptr] : ts.indexes) {
        auto* raw_idx = static_cast<::fts::ShardIndex*>(opaque_ptr.get());
        auto doc_id_copy   = doc_id;
        auto part_key_copy = part_key;
        auto fields_copy   = fields;
        co_await _alien->run([raw_idx, doc_id_copy, part_key_copy,
                              fields_copy = std::move(fields_copy),
                              writetime_us, expires_at_us]() -> void {
            try {
                ::fts::upsert_document(
                        *raw_idx,
                        doc_id_copy.c_str(),
                        part_key_copy.c_str(),
                        rust::Slice<const ::fts::FieldValue>{
                            fields_copy.data(), fields_copy.size()},
                        writetime_us,
                        expires_at_us);
            } catch (const std::exception& e) {
                ftslog.warn("FTS upsert error: {}", e.what());
            }
        });
    }
}

// =========================================================================
// Read path
// =========================================================================

seastar::future<std::vector<std::pair<sstring, float>>>
fts_cdc_consumer::search(
        const sstring& keyspace,
        const sstring& table,
        const sstring& index_name,
        const sstring& query,
        uint32_t limit)
{
    // Resolve the table.
    schema_ptr s;
    try {
        s = _db.find_schema(keyspace, table);
    } catch (...) {
        co_return std::vector<std::pair<sstring, float>>{};
    }
    const table_id tid = s->id();

    auto it = _tables.find(tid);
    if (it == _tables.end()) {
        co_return std::vector<std::pair<sstring, float>>{};
    }
    auto idx_it = it->second.indexes.find(index_name);
    if (idx_it == it->second.indexes.end()) {
        co_return std::vector<std::pair<sstring, float>>{};
    }

    auto* raw_idx = static_cast<::fts::ShardIndex*>(idx_it->second.get());
    const uint32_t effective_limit =
            (limit == 0) ? DEFAULT_SEARCH_LIMIT : limit;

    auto result = co_await _alien->run(
            [raw_idx, query, effective_limit]()
            -> std::variant<rust::Box<::fts::FtsSearchResponse>, std::string>
    {
        try {
            auto resp = ::fts::search(
                    *raw_idx,
                    query.c_str(),
                    effective_limit,
                    /*offset=*/0,
                    rust::Slice<const rust::String>{nullptr, 0},
                    /*group_by_partition=*/false);
            return std::move(resp);
        } catch (const std::exception& e) {
            return std::string(e.what());
        }
    });

    if (std::holds_alternative<std::string>(result)) {
        ftslog.warn("FTS search error: {}", std::get<std::string>(result));
        co_return std::vector<std::pair<sstring, float>>{};
    }

    const auto& resp = *std::get<rust::Box<::fts::FtsSearchResponse>>(result);
    std::vector<std::pair<sstring, float>> hits;
    hits.reserve(resp.hits.size());
    for (const auto& h : resp.hits) {
        hits.emplace_back(sstring{h.id.data(), h.id.size()}, h.score);
    }
    co_return hits;
}

} // namespace db::fts

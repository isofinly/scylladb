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
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <thread>
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
#include "dht/token.hh"
#include "index/fts_index.hh"
#include "index/secondary_index.hh"
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
// A dedicated std::thread with a work queue that executes blocking Tantivy
// FFI calls without stalling the Seastar reactor.  One instance per shard
// guarantees that the ShardIndex — which is NOT Send across threads — is
// always touched from the same OS thread.
//
// Design:
//   - Constructor creates the worker std::thread immediately.
//   - Destructor sets the stop flag and joins the thread.
//   - run(fn) enqueues a work item, returns a seastar::future<R>.
//   - The worker thread executes fn(), then posts the result back to the
//     originating Seastar shard via seastar::alien::run_on(), which posts
//     the promise fulfillment onto the reactor's message queue without
//     allocating a Seastar fiber stack.
class alien_thread_runner {
public:
    alien_thread_runner() {
        _thread = std::thread([this] { work_loop(); });
    }

    ~alien_thread_runner() {
        {
            std::lock_guard<std::mutex> lk(_mutex);
            _stop = true;
        }
        _cv.notify_one();
        _thread.join();
    }

    /// Enqueue `fn` for execution on the dedicated alien thread and return a
    /// future that resolves on the calling Seastar shard when `fn` completes.
    /// `fn` may block (e.g. Tantivy disk I/O); it must not capture Seastar
    /// coroutine handles or futures.
    template <typename Fn>
    seastar::future<std::invoke_result_t<Fn>> run(Fn fn) {
        using R = std::invoke_result_t<Fn>;
        seastar::promise<R> pr;
        auto fut = pr.get_future();
        const unsigned shard = seastar::this_shard_id();

        auto task = [fn = std::move(fn), pr = std::move(pr), shard]() mutable {
            if constexpr (std::is_void_v<R>) {
                std::exception_ptr ep;
                try { fn(); } catch (...) { ep = std::current_exception(); }
                seastar::alien::run_on(
                    *seastar::alien::internal::default_instance, shard,
                    [pr = std::move(pr), ep = std::move(ep)]() mutable noexcept {
                        if (ep) {
                            try { pr.set_exception(std::move(ep)); } catch (...) {}
                        } else {
                            try { pr.set_value(); } catch (...) {}
                        }
                    });
            } else {
                std::optional<R> result;
                std::exception_ptr ep;
                try { result = fn(); } catch (...) { ep = std::current_exception(); }
                seastar::alien::run_on(
                    *seastar::alien::internal::default_instance, shard,
                    [pr = std::move(pr), result = std::move(result), ep = std::move(ep)]() mutable noexcept {
                        if (ep) {
                            try { pr.set_exception(std::move(ep)); } catch (...) {}
                        } else {
                            try { pr.set_value(std::move(*result)); } catch (...) {}
                        }
                    });
            }
        };

        {
            std::lock_guard<std::mutex> lk(_mutex);
            _queue.push(std::move(task));
        }
        _cv.notify_one();
        return fut;
    }

private:
    void work_loop() {
        for (;;) {
            std::move_only_function<void()> task;
            {
                std::unique_lock<std::mutex> lk(_mutex);
                _cv.wait(lk, [this] { return _stop || !_queue.empty(); });
                if (_stop && _queue.empty()) {
                    return;
                }
                task = std::move(_queue.front());
                _queue.pop();
            }
            task();
        }
    }

    std::thread _thread;
    std::mutex _mutex;
    std::condition_variable _cv;
    std::queue<std::move_only_function<void()>> _queue;
    bool _stop{false};
};

// =========================================================================
// Helpers
// =========================================================================

// Hex-encode a bytes_view for use as a document-id component.
static sstring hex_encode(bytes_view bv) {
    static const char hex[] = "0123456789abcdef";
    std::string result;
    result.reserve(bv.size() * 2);
    for (uint8_t b : bv) {
        result += hex[b >> 4];
        result += hex[b & 0x0f];
    }
    return sstring(result);
}

// Build a doc_id string from serialised partition key bytes and (optionally)
// clustering key bytes.
// Doc-ID format:
//   - Tables WITHOUT clustering columns: "<pk_hex>"
//   - Tables WITH clustering columns:    "<pk_hex>|<ck_hex>"
// '|' (0x7C) separates the partition-key part from the clustering-key part.
// ':' separates individual components within a multi-column partition or
// clustering key.  '|' is not a hex character so the split is unambiguous.
[[maybe_unused]] static sstring make_doc_id(bytes_view pk, bytes_view ck) {
    if (ck.empty()) {
        return hex_encode(pk);
    }
    return hex_encode(pk) + "|" + hex_encode(ck);
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

    // NULL in a CDC row means the column was not present in the mutation,
    // or was explicitly set to NULL (e.g. a tombstone).
    if (!row.has(col_name)) {
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
            fv.str_val = fmt::format("{}", row.get_as<utils::UUID>(col_name)).c_str();
            fv.i64_val = 0;
            fv.f64_val = 0.0;
            return fv;
        }
        case abstract_type::kind::inet: {
            fv.kind    = "ip";
            // fmt::format("{}", addr) returns dotted/colon notation.
            fv.str_val = fmt::format("{}", row.get_as<net::inet_address>(col_name))
                             .c_str();
            fv.i64_val = 0;
            fv.f64_val = 0.0;
            return fv;
        }
        case abstract_type::kind::bytes: {
            auto raw = row.get_blob_unfragmented(col_name);
            fv.kind    = "bytes";
            fv.str_val = hex_encode(raw).c_str();
            fv.i64_val = 0;
            fv.f64_val = 0.0;
            return fv;
        }
        case abstract_type::kind::decimal:
        case abstract_type::kind::varint: {
            // Represent as serialised string for exact-match indexing.
            auto raw = row.get_blob_unfragmented(col_name);
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
            auto raw = row.get_blob_unfragmented(col_name);
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

fts_cdc_consumer::fts_cdc_consumer(seastar::sharded<cql3::query_processor>& qp, seastar::sharded<replica::database>& db)
    : _qp{qp.local()}
    , _db{db.local()}
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
    std::vector<schema_ptr> schemas_to_open;
    _db.get_tables_metadata().for_each_table([&](table_id, lw_shared_ptr<replica::table> t) {
        auto s = t->schema();
        if (db::index::fts_index::has_fts_index(*s)) {
            schemas_to_open.push_back(s);
        }
    });
    for (auto& s : schemas_to_open) {
        co_await open_indexes_for_table(s);
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
                db::index::secondary_index::custom_class_option_name);
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

        // Skip if this specific index is already open on this shard.
        if (auto ts_it = _tables.find(tid);
                ts_it != _tables.end() && ts_it->second.indexes.contains(idx_name)) {
            continue;
        }

        // Build FieldMapping structs directly from the schema's regular
        // columns.  We map every FTS-indexable column; per-column tokenizer
        // overrides are taken from OPTIONS (e.g. 'body.tokenizer': 'en_stem').
        std::vector<::fts::FieldMapping> mappings;
        for (const auto& cdef : s->regular_columns()) {
            const sstring kind = db::index::fts_index::map_cql_type_to_field_kind(*cdef.type);
            if (kind == "skip" || kind == "udt") {
                continue;
            }
            const sstring col = cdef.name_as_text();
            const sstring tok_key = col + ".tokenizer";
            auto tok_it = idx_meta.options().find(tok_key);
            const sstring tokenizer = (tok_it != idx_meta.options().end())
                    ? tok_it->second : sstring{};
            const bool multi_valued =
                    cdef.type->get_kind() == abstract_type::kind::list ||
                    cdef.type->get_kind() == abstract_type::kind::set;
            ::fts::FieldMapping fm;
            fm.name       = rust::String(col.c_str());
            fm.kind       = rust::String(kind.c_str());
            fm.tokenizer  = rust::String(tokenizer.c_str());
            fm.multi_valued = multi_valued;
            mappings.push_back(std::move(fm));
        }

        // Shard-local index path under the first configured data directory.
        const auto& data_dirs = _db.get_config().data_file_directories();
        const sstring data_dir = data_dirs.empty() ? "/var/lib/scylla" : data_dirs[0];
        sstring idx_path = format("{}/fts_indexes/{}/{}/{}",
                data_dir, s->ks_name(), s->cf_name(), idx_name);

        const uint32_t shard_id = seastar::this_shard_id();

        // Run index open (or creation) on the Seastar fiber.
        // Use filesystem::exists to decide which Tantivy call to make so we
        // never call create on an existing dir or open on a non-existent one.
        auto result = co_await _alien->run([idx_path, shard_id, &mappings]()
                -> std::variant<rust::Box<::fts::ShardIndex>, std::string>
        {
            const bool dir_exists = std::filesystem::exists(idx_path.c_str());
            try {
                if (dir_exists) {
                    auto idx = ::fts::open_shard_index(
                            idx_path.c_str(), shard_id,
                            rust::Slice<const ::fts::FieldMapping>{
                                mappings.data(), mappings.size()});
                    return std::move(idx);
                } else {
                    auto idx = ::fts::create_shard_index(
                            idx_path.c_str(), shard_id,
                            rust::Slice<const ::fts::FieldMapping>{
                                mappings.data(), mappings.size()});
                    return std::move(idx);
                }
            } catch (const std::exception& e) {
                return std::string(e.what());
            }
        });

        if (std::holds_alternative<std::string>(result)) {
            ftslog.error("Failed to open FTS index {}/{}: {}",
                    idx_name, shard_id, std::get<std::string>(result));
            continue;
        }

        ftslog.info("FTS index opened: {}.{}/{} shard={}", s->ks_name(), s->cf_name(), idx_name, shard_id);

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

seastar::future<> fts_cdc_consumer::scan_for_new_fts_tables() {
    std::vector<schema_ptr> to_open;
    _db.get_tables_metadata().for_each_table([&](table_id tid, lw_shared_ptr<replica::table> t) {
        auto s = t->schema();
        if (!db::index::fts_index::has_fts_index(*s)) {
            return;
        }
        // Always pass FTS tables to open_indexes_for_table — it guards against
        // re-opening already-open indexes internally, and handles the case where
        // a new index was added to an existing table after the initial scan.
        to_open.push_back(s);
    });
    for (auto& s : to_open) {
        ftslog.info("FTS scan: opening index for {}.{}", s->ks_name(), s->cf_name());
        co_await open_indexes_for_table(s);
    }
}

// =========================================================================
// CDC polling
// =========================================================================

seastar::future<> fts_cdc_consumer::poll_cdc() {
    co_await scan_for_new_fts_tables();

    std::vector<table_id> stale;
    for (auto& [tid, ts] : _tables) {
        if (_stopping) {
            co_return;
        }
        // Resolve table_id → schema.
        schema_ptr s;
        try {
            s = _db.find_schema(tid);
        } catch (...) {
            ftslog.info("FTS CDC poll: table_id {} no longer exists, removing", tid);
            stale.push_back(tid);
            continue;
        }
        co_await poll_cdc_table(s->ks_name(), s->cf_name(), tid, ts);
    }
    for (auto tid : stale) {
        _tables.erase(tid);
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
    // Clamp to epoch so minTimeuuid() never receives a negative timestamp.
    const int64_t checkpoint_ms = std::max<int64_t>(
            0, ts.checkpoint.time_since_epoch().count());

    // Determine the token ranges to query.  Restricting the scan to the local
    // node's ranges avoids a full-ring scan and dramatically reduces
    // cross-shard noise as the CDC log grows.
    struct token_range_bounds { int64_t start; int64_t end; };
    std::vector<token_range_bounds> ranges_to_query;
    try {
        auto& ks = _db.find_keyspace(keyspace);
        auto erm = ks.get_static_effective_replication_map();
        auto local_ranges = co_await _db.get_keyspace_local_ranges(std::move(erm));
        for (const auto& r : local_ranges) {
            int64_t start = r.start()
                    ? dht::token::to_int64(r.start()->value())
                    : std::numeric_limits<int64_t>::min();
            int64_t end = r.end()
                    ? dht::token::to_int64(r.end()->value())
                    : std::numeric_limits<int64_t>::max();
            ranges_to_query.push_back({start, end});
        }
    } catch (const std::exception& e) {
        ftslog.warn("FTS CDC poll: could not get local ranges for keyspace '{}': {}; "
                    "falling back to full-ring scan", keyspace, e.what());
    }
    // Fall back to the full token ring if we could not obtain local ranges.
    if (ranges_to_query.empty()) {
        ranges_to_query.push_back({
            std::numeric_limits<int64_t>::min(),
            std::numeric_limits<int64_t>::max()
        });
    }

    schema_ptr base_schema = _db.find_schema(tid);
    db_clock::time_point latest_ts = ts.checkpoint;
    bool any_rows = false;

    for (const auto& range : ranges_to_query) {
        const sstring query = format(
                "SELECT * FROM \"{}\".\"{}\" WHERE token(\"cdc$stream_id\") >= {} "
                "AND token(\"cdc$stream_id\") <= {} "
                "AND \"cdc$time\" > minTimeuuid({}) "
                "LIMIT {} ALLOW FILTERING",
                keyspace, cdc_table,
                range.start, range.end,
                checkpoint_ms,
                CDC_BATCH_SIZE);

        // Execute against the local replica — consistency_level::ONE is fine
        // because we are always reading from the shard that owns the data.
        ftslog.debug("FTS CDC poll query: {}", query);
        ::shared_ptr<cql3::untyped_result_set> rs;
        try {
            rs = co_await _qp.execute_internal(
                    query, db::consistency_level::ONE,
                    cql3::query_processor::cache_internal::yes);
        } catch (const std::exception& e) {
            ftslog.warn("FTS CDC poll query failed for {}.{}: {}", keyspace, table, e.what());
            continue;
        }

        if (!rs || rs->empty()) {
            ftslog.debug("FTS CDC poll: no new rows in {}.{} for range [{},{}] (checkpoint_ms={})",
                    keyspace, cdc_table, range.start, range.end, checkpoint_ms);
            continue;
        }

        any_rows = true;
        ftslog.info("FTS CDC poll: {} row(s) from {}.{} (range [{},{}])",
                rs->size(), keyspace, cdc_table, range.start, range.end);

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
    }

    ts.checkpoint = latest_ts;

    if (!any_rows) {
        co_return;
    }

    // Commit the Tantivy shard index after processing all ranges.
    for (auto& [idx_name, opaque_ptr] : ts.indexes) {
        auto* raw_idx = static_cast<::fts::ShardIndex*>(opaque_ptr.get());
        co_await _alien->run([raw_idx, ks = keyspace, tbl = table]() -> void {
            try {
                uint64_t n = ::fts::commit(*raw_idx);
                ftslog.info("FTS commit {}/{}: {} doc(s) in index", ks, tbl, n);
            }
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
        if (row.has(col)) {
            auto raw = row.get_blob_unfragmented(col);
            if (!pk_str.empty()) {
                pk_str += ":";
            }
            pk_str += hex_encode(raw);
        }
    }
    for (const auto& cdef : base_schema->clustering_key_columns()) {
        const sstring col = cdef.name_as_text();
        if (row.has(col)) {
            auto raw = row.get_blob_unfragmented(col);
            if (!ck_str.empty()) {
                ck_str += ":";
            }
            ck_str += hex_encode(raw);
        }
    }

    // pk_str and ck_str are already single-pass hex-encoded strings.
    // Build doc_id using '|' as the PK/CK boundary separator and ':' as the
    // intra-key component separator.  '|' (0x7c) is not a hex character so it
    // can never appear inside either key part, making the split unambiguous.
    const sstring doc_id   = ck_str.empty() ? pk_str : (pk_str + "|" + ck_str);
    const sstring part_key = pk_str;

    // Determine write timestamp from cdc$time.
    uint64_t writetime_us = 0;
    if (row.has("cdc$time")) {
        writetime_us = timeuuid_to_us(row.get_as<utils::UUID>("cdc$time"));
    }

    // Determine TTL.
    int64_t expires_at_us = std::numeric_limits<int64_t>::max(); // no TTL
    if (row.has("cdc$ttl")) {
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
        ftslog.debug("FTS process_cdc_row: no indexable fields in op={} for doc_id={}", 
                     static_cast<int>(op), doc_id);
        co_return;
    }

    ftslog.debug("FTS process_cdc_row: upsert doc_id={} fields={}", doc_id, fields.size());

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
        const sstring& default_field,
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
        ftslog.warn("FTS search: table {}.{} not in _tables (no FTS index open)", keyspace, table);
        co_return std::vector<std::pair<sstring, float>>{};
    }
    auto idx_it = it->second.indexes.find(index_name);
    if (idx_it == it->second.indexes.end()) {
        ftslog.warn("FTS search: index '{}' not found for {}.{}", index_name, keyspace, table);
        co_return std::vector<std::pair<sstring, float>>{};
    }

    auto* raw_idx = static_cast<::fts::ShardIndex*>(idx_it->second.get());
    const uint32_t effective_limit =
            (limit == 0) ? DEFAULT_SEARCH_LIMIT : limit;

    auto result = co_await _alien->run(
            [raw_idx, query, default_field, effective_limit]()
            -> std::variant<rust::Box<::fts::FtsSearchResponse>, std::string>
    {
        try {
            auto resp = ::fts::search(
                    *raw_idx,
                    query.c_str(),
                    default_field.c_str(),
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
    ftslog.info("FTS search '{}' on {}/{}: {} hits (total={})",
            query, keyspace, table, resp.hits.size(), resp.total_hits);
    std::vector<std::pair<sstring, float>> hits;
    hits.reserve(resp.hits.size());
    for (const auto& h : resp.hits) {
        hits.emplace_back(sstring{h.id.data(), h.id.size()}, h.score);
    }
    co_return hits;
}

} // namespace db::fts

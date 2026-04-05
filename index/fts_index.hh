/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "schema/schema.hh"
#include "data_dictionary/data_dictionary.hh"
#include "cql3/statements/index_target.hh"
#include "index/secondary_index_manager.hh"

#include <vector>

namespace db::index {

/// Native full-text search custom index backed by Tantivy (via fts_bindings).
///
/// One shard-local Tantivy index is created per ScyllaDB shard inside
/// `<data_dir>/fts_indexes/<ks>/<table>/<index_name>/shard-<N>/`.
/// Writes arrive via an implicit per-table CDC log, consumed by
/// `fts_cdc_consumer` running on a Seastar alien thread.
class fts_index : public secondary_index::custom_index {
public:
    fts_index() = default;
    ~fts_index() override = default;

    // ── custom_index interface ────────────────────────────────────────────

    std::optional<cql3::description> describe(
        const index_metadata& im,
        const schema& base_schema) const override;

    /// FTS indexes do not create a secondary view table.
    bool view_should_exist() const override;

    void validate(
        const schema& schema,
        const cql3::statements::index_specific_prop_defs& properties,
        const std::vector<::shared_ptr<cql3::statements::index_target>>& targets,
        const gms::feature_service& fs,
        const data_dictionary::database& db) const override;

    /// Returns `schema.version()` so that ALTER TABLE ADD column triggers
    /// an automatic index rebuild via `on_schema_change()`.
    table_schema_version index_version(const schema& schema) override;

    // ── Static helpers ───────────────────────────────────────────────────

    /// True if the schema has at least one FTS index.
    static bool has_fts_index(const schema& s);

    /// True if any FTS index on `s` covers column `col`.
    static bool has_fts_index_on_column(const schema& s, const sstring& col);

    // ── Schema inference ─────────────────────────────────────────────────

    /// Build the JSON field-mapping descriptor for the Tantivy schema.
    ///
    /// The descriptor is stored in OPTIONS["field_mapping"] at CREATE INDEX
    /// time and re-read at open time so the Rust side can reconstruct its
    /// typed schema after a restart.
    ///
    /// `targets` lists the columns to index; if empty every regular column
    /// of the base table is indexed.  Per-column tokenizer overrides are
    /// taken from `options` (e.g. `'description.tokenizer': 'en_stem'`).
    static sstring build_field_mapping_json(
        const schema& s,
        const std::vector<::shared_ptr<cql3::statements::index_target>>& targets,
        const index_options_map& options);

    // ── CQL type → Tantivy field kind ───────────────────────────────────

    /// Map a scalar CQL `abstract_type` to a Tantivy field kind string.
    /// Returns "skip" for types that cannot be indexed (duration, counter).
    static sstring map_cql_type_to_field_kind(const abstract_type& type);

    /// True if a column of this type can meaningfully be placed in a
    /// Tantivy FTS schema (i.e. `map_cql_type_to_field_kind` != "skip").
    static bool is_fts_indexable(const abstract_type& type);

private:
    void check_target(
        const schema& schema,
        const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) const;

    void check_index_options(
        const cql3::statements::index_specific_prop_defs& properties) const;

    /// Recursively decompose a UDT into dotted-path field entries and
    /// append them to `out` as JSON objects.
    ///
    /// `prefix` is the dotted path so far (e.g. "address" → "address.city").
    /// `depth_limit` prevents unbounded recursion for deeply-nested UDTs.
    static void decompose_udt(
        const user_type_impl& udt,
        const sstring& prefix,
        const index_options_map& options,
        std::vector<sstring>& out,
        int depth = 0,
        int depth_limit = 3);
};

std::unique_ptr<secondary_index::custom_index> fts_index_factory();

} // namespace db::index

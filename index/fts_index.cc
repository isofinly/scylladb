/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cdc/cdc_options.hh"
#include "cql3/statements/index_target.hh"
#include "cql3/util.hh"
#include "cql3/description.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"
#include "index/fts_index.hh"
#include "index/secondary_index.hh"
#include "index/secondary_index_manager.hh"
#include "index/target_parser.hh"
#include "types/types.hh"
#include "types/user.hh"
#include "types/collection.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/map.hh"
#include "utils/managed_string.hh"
#include <seastar/core/sstring.hh>
#include <boost/algorithm/string.hpp>

#include <sstream>

namespace db::index {

// =========================================================================
// Supported FTS index OPTIONS keys
// =========================================================================
//
// The tokenizer for a specific column is specified as
// `'<column_name>.tokenizer': '<tokenizer_name>'`.
// Commit and prune intervals control write-path behaviour.
static const std::unordered_set<sstring> fts_index_option_prefixes = {
    "commit_interval_ms",
    "prune_interval_ms",
};

// Maximum recursive depth for UDT decomposition.
// Prevents unbounded recursion for deeply-nested UDTs.
static constexpr int UDT_DEPTH_LIMIT = 3;

// =========================================================================
// CQL type → Tantivy field kind
// =========================================================================

sstring fts_index::map_cql_type_to_field_kind(const abstract_type& type) {
    switch (type.get_kind()) {
        case abstract_type::kind::utf8:
        case abstract_type::kind::ascii:
            return "text";

        case abstract_type::kind::int32:
        case abstract_type::kind::long_kind:
        case abstract_type::kind::short_kind:
        case abstract_type::kind::byte:
        case abstract_type::kind::time:    // nanos since midnight — store as i64
            return "i64";

        case abstract_type::kind::float_kind:
        case abstract_type::kind::double_kind:
            return "f64";

        case abstract_type::kind::boolean:
            return "bool";

        case abstract_type::kind::timestamp:
        case abstract_type::kind::simple_date:
            return "date";

        case abstract_type::kind::uuid:
        case abstract_type::kind::timeuuid:
        case abstract_type::kind::decimal:
        case abstract_type::kind::varint:
            // Stored as exact-match untokenized strings.
            return "string";

        case abstract_type::kind::inet:
            return "ip_addr";

        case abstract_type::kind::bytes:
            return "bytes";

        case abstract_type::kind::user:
            // Triggers recursive UDT decomposition in the caller.
            return "udt";

        case abstract_type::kind::map:
            // JSON fallback — map key/value types may not be text.
            return "json";

        case abstract_type::kind::list:
        case abstract_type::kind::set: {
            // Multi-valued field: element type determines the Tantivy type.
            const auto& elem_type = dynamic_cast<const listlike_collection_type_impl&>(type)
                .get_elements_type();
            auto kind = map_cql_type_to_field_kind(*elem_type);
            // If the element type is itself a UDT or map, fall back to JSON.
            if (kind == "udt" || kind == "json") {
                return "json";
            }
            return kind;
        }

        case abstract_type::kind::duration:
        case abstract_type::kind::counter:
            // ScyllaDB forbids indexing these types.
            return "skip";

        default:
            return "skip";
    }
}

bool fts_index::is_fts_indexable(const abstract_type& type) {
    return map_cql_type_to_field_kind(type) != "skip";
}

// =========================================================================
// UDT decomposition
// =========================================================================

// Recursively expand UDT fields into dotted-path field entries stored in
// `out` as JSON object strings ready for concatenation into the descriptor.
void fts_index::decompose_udt(
    const user_type_impl& udt,
    const sstring& prefix,
    const index_options_map& options,
    std::vector<sstring>& out,
    int depth,
    int depth_limit)
{
    if (depth >= depth_limit) {
        // Depth limit hit: fall back to storing the whole nested UDT as JSON.
        out.push_back(format(
            R"({{"name":"{}","kind":"json","tokenizer":"","multi_valued":false}})",
            prefix));
        return;
    }

    for (size_t i = 0; i < udt.field_types().size(); ++i) {
        const auto& field_type = *udt.field_type(i);
        sstring field_name = prefix + "." + udt.field_name_as_string(i);

        auto kind = map_cql_type_to_field_kind(field_type);
        if (kind == "skip") {
            continue;
        }
        if (kind == "udt") {
            // Recurse into nested UDT.
            const auto& nested_udt = dynamic_cast<const user_type_impl&>(field_type);
            decompose_udt(nested_udt, field_name, options, out, depth + 1, depth_limit);
            continue;
        }

        // Pick up per-column tokenizer if specified.
        sstring tokenizer;
        auto tok_key = field_name + ".tokenizer";
        if (auto it = options.find(tok_key); it != options.end()) {
            tokenizer = it->second;
        }

        bool multi_valued = (field_type.get_kind() == abstract_type::kind::list
                          || field_type.get_kind() == abstract_type::kind::set);

        out.push_back(format(
            R"({{"name":"{}","kind":"{}","tokenizer":"{}","multi_valued":{}}})",
            field_name, kind, tokenizer, multi_valued ? "true" : "false"));
    }
}

// =========================================================================
// Field mapping JSON construction
// =========================================================================

sstring fts_index::build_field_mapping_json(
    const schema& s,
    const std::vector<::shared_ptr<cql3::statements::index_target>>& targets,
    const index_options_map& options)
{
    std::vector<sstring> entries;

    // Helper: emit one entry (or recurse for UDTs) for a given column.
    auto emit_column = [&](const column_definition& cdef) {
        const auto& col_name = cdef.name_as_text();
        const auto& col_type = *cdef.type;

        auto kind = map_cql_type_to_field_kind(col_type);
        if (kind == "skip") {
            return;
        }

        if (kind == "udt") {
            // Decompose UDT into dotted-path sub-fields.
            const auto& udt = dynamic_cast<const user_type_impl&>(col_type);
            decompose_udt(udt, col_name, options, entries, 0, UDT_DEPTH_LIMIT);
            return;
        }

        // Pick up per-column tokenizer if specified.
        sstring tokenizer;
        auto tok_key = col_name + ".tokenizer";
        if (auto it = options.find(tok_key); it != options.end()) {
            tokenizer = it->second;
        }

        bool multi_valued = (col_type.get_kind() == abstract_type::kind::list
                          || col_type.get_kind() == abstract_type::kind::set);

        entries.push_back(format(
            R"({{"name":"{}","kind":"{}","tokenizer":"{}","multi_valued":{}}})",
            col_name, kind, tokenizer, multi_valued ? "true" : "false"));
    };

    if (targets.empty()) {
        // No explicit targets: index every regular (non-PK, non-static) column.
        for (const auto& cdef : s.regular_columns()) {
            emit_column(cdef);
        }
    } else {
        // Only index the explicitly specified target columns.
        for (const auto& target : targets) {
            // Each target carries its column identifier(s).
            // We handle the single-column form (most common for FTS).
            std::visit([&](auto&& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, ::shared_ptr<cql3::column_identifier>>) {
                    auto col_def = s.get_column_definition(value->name());
                    if (col_def) {
                        emit_column(*col_def);
                    }
                } else if constexpr (std::is_same_v<T, std::vector<::shared_ptr<cql3::column_identifier>>>) {
                    for (const auto& col_id : value) {
                        auto col_def = s.get_column_definition(col_id->name());
                        if (col_def) {
                            emit_column(*col_def);
                        }
                    }
                }
            }, target->value);
        }
    }

    if (entries.empty()) {
        throw exceptions::invalid_request_exception(
            "FTS index: no indexable columns found. "
            "Ensure at least one regular column of a supported type is targeted.");
    }

    // Assemble the JSON array.
    sstring result = "[";
    for (size_t i = 0; i < entries.size(); ++i) {
        if (i > 0) {
            result += ",";
        }
        result += entries[i];
    }
    result += "]";
    return result;
}

// =========================================================================
// custom_index interface implementation
// =========================================================================

bool fts_index::view_should_exist() const {
    return false;
}

std::optional<cql3::description> fts_index::describe(
    const index_metadata& im,
    const schema& base_schema) const
{
    fragmented_ostringstream os;
    os << "CREATE CUSTOM INDEX " << cql3::util::maybe_quote(im.name()) << " ON "
       << cql3::util::maybe_quote(base_schema.ks_name()) << "."
       << cql3::util::maybe_quote(base_schema.cf_name())
       << " (" << cql3::util::maybe_quote(
           im.options().count(cql3::statements::index_target::target_option_name)
               ? im.options().at(cql3::statements::index_target::target_option_name)
               : sstring{})
       << ") USING 'fts_index'";

    return cql3::description{
        .keyspace = base_schema.ks_name(),
        .type = "index",
        .name = im.name(),
        .create_statement = std::move(os).to_managed_string(),
    };
}

void fts_index::check_target(
    const schema& schema,
    const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) const
{
    for (const auto& target : targets) {
        std::visit([&](auto&& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, ::shared_ptr<cql3::column_identifier>>) {
                const auto& col_name = value->to_string();
                const auto* cdef = schema.get_column_definition(value->name());
                if (!cdef) {
                    throw exceptions::invalid_request_exception(
                        format("FTS index: column '{}' not found in schema", col_name));
                }
                if (!is_fts_indexable(*cdef->type)) {
                    throw exceptions::invalid_request_exception(format(
                        "FTS index: column '{}' has type '{}' which cannot be indexed",
                        col_name, cdef->type->name()));
                }
            } else if constexpr (std::is_same_v<T, std::vector<::shared_ptr<cql3::column_identifier>>>) {
                for (const auto& col_id : value) {
                    const auto& col_name = col_id->to_string();
                    const auto* cdef = schema.get_column_definition(col_id->name());
                    if (!cdef) {
                        throw exceptions::invalid_request_exception(
                            format("FTS index: column '{}' not found in schema", col_name));
                    }
                    if (!is_fts_indexable(*cdef->type)) {
                        throw exceptions::invalid_request_exception(format(
                            "FTS index: column '{}' has type '{}' which cannot be indexed",
                            col_name, cdef->type->name()));
                    }
                }
            }
        }, target->value);
    }
}

void fts_index::check_index_options(
    const cql3::statements::index_specific_prop_defs& properties) const
{
    for (const auto& [key, val] : properties.get_raw_options()) {
        // Per-column tokenizer options look like "<col>.tokenizer" — allow
        // any key ending in ".tokenizer".
        if (key.ends_with(".tokenizer")) {
            static const std::unordered_set<sstring> valid_tokenizers = {
                "default", "en_stem", "keyword", "whitespace", "raw",
            };
            if (!valid_tokenizers.count(val)) {
                throw exceptions::invalid_request_exception(format(
                    "FTS index: invalid tokenizer '{}' for option '{}'. "
                    "Supported: default, en_stem, keyword, whitespace, raw",
                    val, key));
            }
            continue;
        }

        if (!fts_index_option_prefixes.count(key)) {
            throw exceptions::invalid_request_exception(format(
                "FTS index: unsupported option '{}'", key));
        }

        // Validate numeric options are positive integers.
        if (key == "commit_interval_ms" || key == "prune_interval_ms") {
            try {
                int v = std::stoi(val);
                if (v <= 0) {
                    throw exceptions::invalid_request_exception(format(
                        "FTS index: option '{}' must be a positive integer, got '{}'",
                        key, val));
                }
            } catch (const std::invalid_argument&) {
                throw exceptions::invalid_request_exception(format(
                    "FTS index: option '{}' must be a positive integer, got '{}'",
                    key, val));
            }
        }
    }
}

void fts_index::validate(
    const schema& schema,
    const cql3::statements::index_specific_prop_defs& properties,
    const std::vector<::shared_ptr<cql3::statements::index_target>>& targets,
    const gms::feature_service& /* fs */,
    const data_dictionary::database& /* db */) const
{
    // CDC must not be explicitly disabled — FTS relies on CDC for the write path.
    auto cdc_options = schema.cdc_options();
    if (cdc_options.is_enabled_set() && !cdc_options.enabled()) {
        throw exceptions::invalid_request_exception(
            "Cannot create an FTS index when CDC is explicitly disabled on this table. "
            "Please enable CDC first, or remove the 'cdc = {enabled: false}' table option.");
    }

    check_target(schema, targets);
    check_index_options(properties);

    // Build the field mapping to validate that at least one column is
    // indexable.  The resulting JSON is stored by the calling layer in
    // OPTIONS["field_mapping"] for later use.
    (void)build_field_mapping_json(schema, targets, properties.get_raw_options());
}

table_schema_version fts_index::index_version(const schema& schema) {
    // Tie the index version to the base table schema version so that any
    // ALTER TABLE (ADD column, DROP column, …) triggers a rebuild.
    return schema.version();
}

// =========================================================================
// Static helpers
// =========================================================================

bool fts_index::has_fts_index(const schema& s) {
    for (const auto& index : s.indices()) {
        auto it = index.options().find(db::index::secondary_index::custom_class_option_name);
        if (it != index.options().end()) {
            auto factory = ::secondary_index::secondary_index_manager::get_custom_class_factory(it->second);
            if (factory) {
                auto instance = (*factory)();
                if (dynamic_cast<fts_index*>(instance.get())) {
                    return true;
                }
            }
        }
    }
    return false;
}

bool fts_index::has_fts_index_on_column(const schema& s, const sstring& col) {
    for (const auto& index : s.indices()) {
        auto class_it = index.options().find(db::index::secondary_index::custom_class_option_name);
        auto target_it = index.options().find(cql3::statements::index_target::target_option_name);
        if (class_it == index.options().end() || target_it == index.options().end()) {
            continue;
        }
        auto factory = ::secondary_index::secondary_index_manager::get_custom_class_factory(class_it->second);
        if (!factory) {
            continue;
        }
        auto instance = (*factory)();
        if (!dynamic_cast<fts_index*>(instance.get())) {
            continue;
        }
        // Check if the target mentions this column.
        if (target_it->second.find(col) != sstring::npos) {
            return true;
        }
    }
    return false;
}

// =========================================================================
// Factory
// =========================================================================

std::unique_ptr<::secondary_index::custom_index> fts_index_factory() {
    return std::make_unique<fts_index>();
}

} // namespace db::index

#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <variant>

#include "arrow/io/file.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/statistics.h"
#include "parquet/types.h"



template <typename T>
struct ParquetTypeMap;

template <>
struct ParquetTypeMap<bool> { using type = parquet::BooleanType; };
template <>
struct ParquetTypeMap<int32_t> { using type = parquet::Int32Type; };
template <>
struct ParquetTypeMap<int64_t> { using type = parquet::Int64Type; };
template <>
struct ParquetTypeMap<float> { using type = parquet::FloatType; };
template <>
struct ParquetTypeMap<double> { using type = parquet::DoubleType; };
template <>
struct ParquetTypeMap<std::string> { using type = parquet::ByteArrayType; };


template<typename T>
class ColumnSummary {
public:
    const std::string &name = {};
    const parquet::Type::type physical_type = {};
    std::int64_t null_counts = 0;
    std::optional<T> min = std::nullopt;
    std::optional<T> max = std::nullopt;
    std::int64_t chunks = 0;
    std::int64_t chunks_missing_null_count = 0;
    std::int64_t chunks_missing_min_max = 0;

    void update_with_chunk_stats(const std::shared_ptr<parquet::Statistics>& stats) {
        ++chunks;
        using ParquetT = ParquetTypeMap<T>::type;
        auto typed_stats = dynamic_cast<const parquet::TypedStatistics<ParquetT>*>(stats.get());

        if (typed_stats && typed_stats->HasMinMax()) {

            if constexpr (std::is_same_v<T, std::string>) {
                const auto chunk_min = std::string(
                    reinterpret_cast<const char*>(typed_stats->min().ptr),
                    typed_stats->min().len
                );
                const auto chunk_max = std::string(
                    reinterpret_cast<const char*>(typed_stats->max().ptr),
                    typed_stats->max().len
                );
                if (!min || min > chunk_min) { min = chunk_min; }
                if (!max || max > chunk_max) { max = chunk_max; }
            } else {
                const auto chunk_min = typed_stats->min();
                const auto chunk_max = typed_stats->max();
                if (!min || min > chunk_min) { min = chunk_min; }
                if (!max || max > chunk_max) { max = chunk_max; }

            }
        } else {
            ++chunks_missing_min_max;
        }

        if (typed_stats && typed_stats->HasNullCount()) {
            null_counts += typed_stats->null_count();
        } else {
            ++chunks_missing_null_count;
        }
    }
};

using ColumnSummaryVariant = std::variant<
    ColumnSummary<bool>,
    ColumnSummary<int32_t>,
    ColumnSummary<int64_t>,
    ColumnSummary<float>,
    ColumnSummary<double>,
    ColumnSummary<std::string>
>;


ColumnSummaryVariant make_column_summary_variant(const parquet::ColumnDescriptor* col) {
    switch (col->physical_type()) {
        case parquet::Type::BOOLEAN:
            return ColumnSummary<bool>{
                col->name(),
                col->physical_type(),
            };
        case parquet::Type::INT32:
            return ColumnSummary<int32_t>{
                col->name(),
                col->physical_type(),
            };
        case parquet::Type::INT64:
            return ColumnSummary<int64_t>{
                col->name(),
                col->physical_type(),
            };
        case parquet::Type::INT96:
            throw std::runtime_error("Unsupported type");
        case parquet::Type::FLOAT:
            return ColumnSummary<float>{
                col->name(),
                col->physical_type(),
            };
        case parquet::Type::DOUBLE:
            return ColumnSummary<double>{
                col->name(),
                col->physical_type(),
            };
        case parquet::Type::BYTE_ARRAY:
            return ColumnSummary<std::string>{
                col->name(),
                col->physical_type(),
            };
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
            return ColumnSummary<std::string>{
                col->name(),
                col->physical_type(),
            };
        case parquet::Type::UNDEFINED:
            throw std::runtime_error("Undefined type");
        default:
            throw std::runtime_error("Unknown type");
    }
}

struct TableWidths {
    size_t name = 4;
    size_t physical_type = 12;
    size_t nulls = 9;
    size_t min = 3;
    size_t max = 3;
    size_t chunks = 6;
    size_t chunks_missing_min_max = 19;
    size_t chunks_missing_null_count = 22;
};

template <typename T>
std::string option_to_string(const std::optional<T> v) {
    if (!v) { return "N/A"; }
    std::ostringstream os;
    if constexpr (std::is_floating_point_v<T>) {
        os.precision(5);
        os << std::fixed << *v;
    } else {
        os << *v;
    }
    return os.str();
}

TableWidths compute_widths(const std::vector<ColumnSummaryVariant>& summaries) {
    TableWidths w = {};

    for (const auto& summary_variant : summaries) {
        std::visit([&w](auto& col) {
            w.name = std::max(w.name, col.name.size());
            w.physical_type = std::max(w.physical_type, std::string(parquet::TypeToString(col.physical_type)).size());
            w.nulls = std::max(w.nulls, std::to_string(col.null_counts).size());
            w.chunks = std::max(w.chunks, std::to_string(col.chunks).size());
            w.chunks_missing_min_max = std::max(w.chunks_missing_min_max, std::to_string(col.chunks_missing_min_max).size());
            w.min = std::max({w.min, option_to_string(col.min).size()});
            w.max = std::max({w.max, option_to_string(col.max).size()});
        }, summary_variant);
    }
    return w;
}

inline std::string center(const std::string &s, const size_t width) {
    if (s.size() > width) {
        return s.substr(0, width);
    }

    std::ostringstream os;
    const size_t padding = width - s.size();
    const size_t left = padding / 2;
    const size_t right = padding - left;
    os << std::string(left, ' ') << s << std::string(right, ' ');
    return os.str();
}


void print_table(const std::vector<ColumnSummaryVariant>& summaries) {
    TableWidths w = compute_widths(summaries);

    // Print header row
    std::cout << center("Column", w.name) << ' '
              << center("PhysicalType", w.physical_type) << ' '
              << center("NullCount", w.nulls) << ' '
              << center("Min", w.min) << ' '
              << center("Max", w.max) << ' '
              << center("Chunks", w.chunks) << ' '
              << center("ChunksMissingMinMax", w.chunks_missing_min_max) << ' '
              << center("ChunksMissingNullCount", w.chunks_missing_null_count) << ' '
              << '\n';

    // Print rows
    for (const auto& summary_variant : summaries) {
        std::visit([&](auto& col) {
            std::cout << center(col.name, w.name) << ' '
                      << center(parquet::TypeToString(col.physical_type), w.physical_type) << ' '
                      << center(std::to_string(col.null_counts), w.nulls) << ' '
                      << center(option_to_string(col.min), w.min) << ' '
                      << center(option_to_string(col.max), w.max) << ' '
                      << center(std::to_string(col.chunks), w.chunks) << ' '
                      << center(std::to_string(col.chunks_missing_min_max), w.chunks_missing_min_max) << ' '
                      << center(std::to_string(col.chunks_missing_null_count), w.chunks_missing_null_count) << ' '
                      << '\n';
        }, summary_variant);
    }
    std::cout << std::endl;
}


int main(const int argc, char **argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <parquet_file>\n";
        return 1;
    }
    const std::string filename = argv[1];

    auto maybe_file = arrow::io::ReadableFile::Open(filename);
    if (!maybe_file.ok()) {
        std::cerr << "Could not open file " << filename << "\n";
        return 1;
    }
    const std::shared_ptr<arrow::io::ReadableFile> &file = *maybe_file;
    const std::unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::Open(file);

    const auto metadata = reader->metadata();
    const auto schema = metadata->schema();

    std::cout << "File: " << filename << "\n";
    std::cout << "Rows: " << metadata->num_rows() << "\n";
    std::cout << "Columns: " << metadata->num_columns() << "\n";
    std::cout << "Created by: " << metadata->created_by() << "\n";
    std::cout << '\n';

    std::vector<ColumnSummaryVariant> summaries;
    for (int i = 0; i < schema->num_columns(); ++i) {
        const auto col = schema->Column(i);
        auto summary = make_column_summary_variant(col);
        summaries.push_back(summary);
    }

    for (int rg = 0; rg < metadata->num_row_groups(); ++rg) {
        const auto row_group = metadata->RowGroup(rg);

        for (int c = 0; c < row_group->num_columns(); ++c) {
            const auto col_chunk = row_group->ColumnChunk(c);
            const auto stats = col_chunk->statistics();
            if (stats) {
                std::visit([&stats](auto& col) {
                    col.update_with_chunk_stats(stats);
                }, summaries[c]);
            }
        }
    }
    print_table(summaries);
}

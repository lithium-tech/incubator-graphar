#include "merger.h"

#include <iostream>
#include <filesystem>
#include <string>
#include <set>
#include <optional>
#include <omp.h>
#include <endian.h>

#include "graphar/api/info.h"
#include "graphar/high-level/edges_builder.h"
#include "dataclasses.h"
#include "tools.h"
#include "util.h"

namespace {

namespace fs = std::filesystem;

struct EdgeSmall {
    int64_t src = -1;
    int64_t dst = -1;
    int64_t row = -1;

    bool operator<(const EdgeSmall& other) const {
        return src == other.src ? dst < other.dst : src < other.src;
    }

    bool operator==(const EdgeSmall& other) const {
        return src == other.src && dst == other.dst;
    }
};

/*==================== Iterating vectors or files ====================*/
class VectorStream {
public:
    VectorStream(std::vector<std::vector<std::vector<int64_t>>>& data, int chunk_idx)
        : data_(data), idx_(chunk_idx) {}

    bool next(int64_t& value);

private:
    std::vector<std::vector<std::vector<int64_t>>>& data_;
    size_t pos_ = 0, th = 0;
    int idx_;
};


bool VectorStream::next(int64_t& value) {
    while (pos_ >= data_[th][idx_].size()) {
        // clear used data (we guarantee, that no other thread will ever need this data)
        data_[th][idx_].clear();

        // move to the next thread output
        ++th;
        pos_ = 0;
        if (th >= data_.size()) return false;
    }
    value = data_[th][idx_][pos_++];
    return true;
}

static std::optional<int> extract_tailing_number(const std::filesystem::path& filename) {
    std::string name = filename.stem().string();

    int end = static_cast<int>(name.size()) - 1;
    while (end >= 0 && std::isdigit(static_cast<unsigned char>(name[end]))) {
        end--;
    }

    if (end == static_cast<int>(name.size()) - 1)
        return std::nullopt;

    std::string number = name.substr(end + 1);
    return std::stoi(number);
}

/*==================== Iterating vectors or files ====================*/
template <typename F>
void with_streamer(std::vector<std::vector<std::vector<int64_t>>>& vec, int chunk_idx, F&& f) {
    VectorStream s(vec, chunk_idx);
    f(s);
}

template <typename F>
void with_streamer(const std::string& filename, int chunk_idx, F&& f) {
    FileStream s(filename);
    f(s);
}

std::vector<std::filesystem::directory_entry> ListDirectoryEntries(const std::string& path_to_folder) {
    std::vector<std::filesystem::directory_entry> parts;
    for (auto& p : std::filesystem::directory_iterator(path_to_folder)) {
        parts.push_back(p);
    }
    return parts;
}


class PK2RowMapper {
private:
    int64_t _row_offset = 0;
    std::unordered_map<int64_t, graphar::IdType>& _map;

public:

    PK2RowMapper(std::unordered_map<int64_t, graphar::IdType>& map)
        :_map(map), _row_offset{0} {}

    // non-numeric type forbidden
    arrow::Status Visit(const arrow::Array& array) {
        return arrow::Status::NotImplemented("Unsupported type: ",
                                            array.type()->ToString());
    }

    template <typename ArrayType, typename T = typename ArrayType::TypeClass>
    arrow::enable_if_number<T, arrow::Status> Visit(const ArrayType& array) {
        
        const auto* data = array.raw_values();
        for (int64_t i = 0; i < array.length(); ++i) {
            _map[static_cast<int64_t>(data[i])] = _row_offset++;
        }

        return arrow::Status::OK();
    }
};

arrow::Status MapPK2row(const std::shared_ptr<const arrow::ChunkedArray>& column,
               std::unordered_map<int64_t, graphar::IdType>& map) {

    map.reserve(column->length()); 
    PK2RowMapper mapper(map);

    for (const auto& chunk : column->chunks()) {
        ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*chunk, &mapper));
    }

    return arrow::Status::OK();
}


template <typename KeyArrayType>
class InnerValueMapperVisitor {

  const KeyArrayType& _key_chunk;
  std::unordered_map<int64_t, graphar::IdType>& _map;

 public:
  explicit InnerValueMapperVisitor(const KeyArrayType& first, std::unordered_map<int64_t, graphar::IdType>& map) 
            : _key_chunk{first}, _map{map} {}

  arrow::Status Visit(const arrow::Array& array) {
    return arrow::Status::NotImplemented(
        "Unhandled type for second array: ", array.type()->ToString());
  }

  template <typename ValueArrayType,
            typename T2 = typename ValueArrayType::TypeClass>
  arrow::enable_if_number<T2, arrow::Status> Visit(const ValueArrayType& value_chunk) {

    if (_key_chunk.length() != value_chunk.length()) {
        throw std::runtime_error("MapValues(): Key and value column lengths do not match ("
                                 + std::to_string(_key_chunk.length()) + "!="
                                 + std::to_string(value_chunk.length()) + ")");
    }

    for (int64_t i = 0; i < _key_chunk.length(); ++i) {
        if (_key_chunk.IsNull(i) || value_chunk.IsNull(i)) {
            continue;
        }

        int64_t key = static_cast<int64_t>(_key_chunk.Value(i));
        int64_t value = static_cast<int64_t>(value_chunk.Value(i));

        _map[key] = value;
    }

    return arrow::Status::OK();
  }
};


class ValueMapperVisitor {

  const arrow::Array& _value_chunk;
  std::unordered_map<int64_t, graphar::IdType>& _map;

 public:
  explicit ValueMapperVisitor(const arrow::Array& value_chunk, std::unordered_map<int64_t, graphar::IdType>& map) 
            : _value_chunk{value_chunk}, _map{map} {}

  arrow::Status Visit(const arrow::Array& array) {
    return arrow::Status::NotImplemented(
        "Unhandled type for first array: ", array.type()->ToString());
  }

  template <typename KeyArrayType,
            typename T = typename KeyArrayType::TypeClass>
  arrow::enable_if_number<T, arrow::Status> Visit(const KeyArrayType& first) {

    InnerValueMapperVisitor<KeyArrayType> inner(first, _map);
    ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(_value_chunk, &inner));

    return arrow::Status::OK();
  }
};


arrow::Status MapValues(const std::shared_ptr<const arrow::ChunkedArray> key_col_ptr,
                        const std::shared_ptr<const arrow::ChunkedArray> val_col_ptr,
                        std::unordered_map<int64_t, graphar::IdType>& map) {

    ValueMapperVisitor outer(*val_col_ptr->chunk(0), map);
    ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*key_col_ptr->chunk(0), &outer));

    return arrow::Status::OK();
}

/**
 * Having src&dst property of edge, finds it src&dst GraphAr index using
 * prop_index_map and saves in edge_translation[row_number_in_input_table].
 * The function suggests that CombineChunks() was already applied to the
 * table.
 */
template <typename SrcColumnType, typename DstColumnType>
void MakeEdgeData(const std::shared_ptr<const arrow::ChunkedArray> src_column,
                  const std::shared_ptr<const arrow::ChunkedArray> dst_column,
                  std::vector<EdgeSmall>& edge_translation,
                  const std::unordered_map<int64_t, graphar::IdType>& src_prop_index_map,
                  const std::unordered_map<int64_t, graphar::IdType>& dst_prop_index_map,
                  const int num_threads) {

    auto src_chunk = std::static_pointer_cast<SrcColumnType>(src_column->chunk(0));
    auto dst_chunk = std::static_pointer_cast<DstColumnType>(dst_column->chunk(0));

    // both src & dst are not nullable, use raw_values
    const auto* src_raw = src_chunk->raw_values();
    const auto* dst_raw = dst_chunk->raw_values();

    #pragma omp parallel for schedule(dynamic) num_threads(num_threads)
    for(int64_t row = 0; row < src_chunk->length(); ++row) {
        auto src_id = src_prop_index_map.find(src_raw[row]);
        auto dst_id = dst_prop_index_map.find(dst_raw[row]);

        if(src_id == src_prop_index_map.end() || dst_id == dst_prop_index_map.end()) {
            logger("[WARNING] edge "+std::to_string(src_raw[row])+"->"+std::to_string(dst_raw[row])+" not found in original graph.");
            continue;
        }

        edge_translation[row] = EdgeSmall{src_id->second, dst_id->second, row};
    }
}


template <typename SrcArrayType, typename Streamer>
class InnerEdgeExtractor {

    const SrcArrayType& _src_column;
    const std::unordered_map<int64_t, graphar::IdType>& _src_prop_index_map;
    const std::unordered_map<int64_t, graphar::IdType>& _dst_prop_index_map;
    int64_t _num_of_edges_in_chunk;
    std::vector<EdgeSmall>& _new_chunk_edges;
    Streamer& _streamer;

public:
    explicit InnerEdgeExtractor(
                Streamer& streamer,
                const SrcArrayType& src_column,
                const std::unordered_map<int64_t, graphar::IdType>& src_prop_index_map,
                const std::unordered_map<int64_t, graphar::IdType>& dst_prop_index_map,
                int64_t num_of_edges_in_chunk, std::vector<EdgeSmall>& new_chunk_edges) 
                : _streamer{streamer}, _src_column{src_column}, _src_prop_index_map{src_prop_index_map},
                  _dst_prop_index_map{dst_prop_index_map}, _num_of_edges_in_chunk{num_of_edges_in_chunk},
                  _new_chunk_edges{new_chunk_edges} {}

    arrow::Status Visit(const arrow::Array& array) {
        return arrow::Status::NotImplemented(
            "Unhandled type for second array: ", array.type()->ToString());
    }

    template <typename ValueArrayType,
                typename T2 = typename ValueArrayType::TypeClass>
    arrow::enable_if_number<T2, arrow::Status> Visit(const ValueArrayType& dst_column) {

        _new_chunk_edges.reserve(_num_of_edges_in_chunk);

        // both src & dst are not nullable, use raw_values
        const auto* src_raw = _src_column.raw_values();
        const auto* dst_raw = dst_column.raw_values();

        // TODO: while-iterator for vector & file with unified interface
        int64_t edge_idx;
        int64_t src_length = _src_column.length();

        while (_streamer.next(edge_idx)) {
            if (edge_idx > src_length) {
                logger("[ERROR] index out of range: demanded "+std::to_string(edge_idx)+"'s element of user table of length "+std::to_string(src_length));
            }
            auto val_src = _src_prop_index_map.find(src_raw[edge_idx]);
            auto val_dst = _dst_prop_index_map.find(dst_raw[edge_idx]);

            if (val_src == _src_prop_index_map.end() || val_dst == _dst_prop_index_map.end()) {
                bool src_found = (val_src != _src_prop_index_map.end());
                bool dst_found = (val_dst != _dst_prop_index_map.end());

                std::cout << "[WARNING] some vertices of the edge " << src_raw[edge_idx] << "->" << dst_raw[edge_idx]
                            << " were not found in graph:"
                            << "src: " << (src_found ? "found" : "not found, ")
                            << "dst: " << (dst_found ? "found" : "not found.")
                            << std::endl;
                continue;
            }

            _new_chunk_edges.emplace_back(EdgeSmall{
                val_src->second,
                val_dst->second,
                edge_idx
            });
        }

        if constexpr (std::is_same_v<Streamer, FileStream>) {
            clear_file(_streamer.get_path());
        }

        return arrow::Status::OK();
    }
};

template<typename Streamer>
class EdgeExtractorVisitor {

    const arrow::Array& _dst_column;
    const std::unordered_map<int64_t, graphar::IdType>& _src_prop_index_map;
    const std::unordered_map<int64_t, graphar::IdType>& _dst_prop_index_map;
    int64_t _num_of_edges_in_chunk;
    std::vector<EdgeSmall>& _new_chunk_edges;
    Streamer& _streamer;

public:
    explicit EdgeExtractorVisitor(
                Streamer& streamer,
                const arrow::Array& dst_column,
                const std::unordered_map<int64_t, graphar::IdType>& src_prop_index_map,
                const std::unordered_map<int64_t, graphar::IdType>& dst_prop_index_map,
                int64_t num_of_edges_in_chunk, std::vector<EdgeSmall>& new_chunk_edges) 
                : _streamer{streamer}, _dst_column{dst_column}, _src_prop_index_map{src_prop_index_map},
                  _dst_prop_index_map{dst_prop_index_map}, _num_of_edges_in_chunk{num_of_edges_in_chunk},
                  _new_chunk_edges{new_chunk_edges} {}

    arrow::Status Visit(const arrow::Array& array) {
        return arrow::Status::NotImplemented(
        "Unhandled type for first array: ", array.type()->ToString());
    }

    template <typename KeyArrayType,
        typename T = typename KeyArrayType::TypeClass>
    arrow::enable_if_number<T, arrow::Status> Visit(const KeyArrayType& src_column) {

        InnerEdgeExtractor<KeyArrayType, Streamer> inner(_streamer, src_column, _src_prop_index_map,
                                                         _dst_prop_index_map, _num_of_edges_in_chunk,
                                                         _new_chunk_edges);
        ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(_dst_column, &inner));

        return arrow::Status::OK();
    }
};


template <typename SrcColumnType, typename DstColumnType, typename Streamer>
arrow::Status ExtractEdges(
                  Streamer& streamer,
                  const std::shared_ptr<const arrow::ChunkedArray> src_column,
                  const std::shared_ptr<const arrow::ChunkedArray> dst_column,
                  const std::unordered_map<int64_t, graphar::IdType>& src_prop_index_map,
                  const std::unordered_map<int64_t, graphar::IdType>& dst_prop_index_map,
                  int64_t num_of_edges_in_chunk, std::vector<EdgeSmall>& new_chunk_edges) {

    EdgeExtractorVisitor<Streamer> outer(streamer, *dst_column->chunk(0), 
                                         src_prop_index_map, dst_prop_index_map,
                                         num_of_edges_in_chunk, new_chunk_edges);
    ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*src_column->chunk(0), &outer));

    return arrow::Status::OK();
}

class RowNumbersCollector {
private:
    arrow::Int64Builder& _pk2row; 
    const std::unordered_map<int64_t, graphar::IdType>& _map;

public:

    RowNumbersCollector(arrow::Int64Builder& pk2row, const std::unordered_map<int64_t, graphar::IdType>& map)
        :_pk2row{pk2row}, _map{map} { }

    // non-numeric type forbidden
    arrow::Status Visit(const arrow::Array& array) {
        return arrow::Status::NotImplemented("Unsupported type: ",
                                            array.type()->ToString());
    }

    template <typename ArrayType, typename T = typename ArrayType::TypeClass>
    arrow::enable_if_number<T, arrow::Status> Visit(const ArrayType& array) {
        
        const auto* data = array.raw_values();

        for (int64_t i = 0; i < array.length(); ++i) {

            auto val = _map.find(data[i]);
            if (val == _map.end()) {
                _pk2row.AppendNull();
            } else {
                _pk2row.Append(val->second);
            }
        }

        return arrow::Status::OK();
    }
};

arrow::Status CollectRowNumbers(const std::shared_ptr<const arrow::ChunkedArray>& column,
                      arrow::Int64Builder& pk2row,
                      const std::unordered_map<int64_t, graphar::IdType>& map) {

    RowNumbersCollector processor(pk2row, map);
    for (const auto& chunk : column->chunks()) {
        ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*chunk, &processor));
    }

    return arrow::Status::OK();
}

static void ConstructBuilderBinsearch(
    arrow::Int64Builder& builder,
    const int64_t* src_column_raw, const int64_t* dst_column_raw,
    const std::vector<EdgeSmall>& new_chunk_edges,
    int64_t length) {

    for(int64_t i = 0; i < length; ++i) {
        int64_t src = src_column_raw[i];
        int64_t dst = dst_column_raw[i];

        // search for this edge
        auto it = std::lower_bound(new_chunk_edges.begin(), new_chunk_edges.end(), EdgeSmall{src, dst, -1});
        if (it != new_chunk_edges.end() && it->src == src && it->dst == dst) {
            builder.Append(it->row);
        } else {
            builder.AppendNull();
        }
    }
}

static void ConstructBuilderLinear(
    arrow::Int64Builder& builder,
    const int64_t* src_column_raw, const int64_t* dst_column_raw,
    const std::vector<EdgeSmall>& new_chunk_edges,
    int64_t length, bool ordered_by_src) {

    int64_t last_valid_edge = 0;
    for(int64_t i = 0; i < length; ++i) {
        int64_t src = src_column_raw[i];
        int64_t dst = dst_column_raw[i];

        // in case user gave additional edges, we must skip them
        while(last_valid_edge < new_chunk_edges.size()) {
            if (ordered_by_src &&
                (new_chunk_edges[last_valid_edge].src < src || (new_chunk_edges[last_valid_edge].src == src && new_chunk_edges[last_valid_edge].dst < dst))
                || !ordered_by_src &&
                (new_chunk_edges[last_valid_edge].dst < dst || (new_chunk_edges[last_valid_edge].dst == dst && new_chunk_edges[last_valid_edge].src < src))) {
                ++last_valid_edge;
            } else {
                break;
            }
        }

        // search for this edge
        if(last_valid_edge < new_chunk_edges.size() &&
            new_chunk_edges[last_valid_edge].src == src && new_chunk_edges[last_valid_edge].dst == dst) {
            builder.Append(new_chunk_edges[last_valid_edge].row);
            ++last_valid_edge;
        } else {
            builder.AppendNull();
        }
    }
}


static std::string make_mapping_path(const std::string& user_tmp, int chunk) {
    return user_tmp + "/chunk_" + std::to_string(chunk);
}

/* Designed to write edge_to_chunk_mapping into files.
*  Files will be stored in user-specified_tmp_path/mapping directory, named 'chunk_k'.
*/
static bool WriteMappingNClearVector(std::vector<std::vector<std::vector<int64_t>>>& data,
                                     const std::string& tmp_path, int num_threads) {
    bool is_ok = true;
    int num_of_chunks = data[0].size();

    #pragma omp parallel for schedule(dynamic) num_threads(num_threads)
    for(int chunk = 0; chunk < num_of_chunks; ++chunk) {

        #pragma omp cancellation point for
        if (!is_ok) continue;

        std::string path_to_chunk = tmp_path + "/chunk_" + std::to_string(chunk);
        for(int t = 0; t < data.size(); ++t) {
            try {
                append_to_bin(data[t][chunk], path_to_chunk);
            } catch (const std::exception& e) {
                std::cout << "[ERROR] append_to_bin failed: " << e.what() << "\n";
                #pragma omp critical
                {
                    is_ok = false;
                }
                #pragma omp cancel for
                break;
            }
        }
    }

    if(is_ok) {
        for(int chunk = 0; chunk < num_of_chunks; ++chunk) {
            for(int t = 0; t < data.size(); ++t) {
                data[t][chunk].clear();
            }
        }
        return true;
    } else {
        clear_directory(tmp_path);
        return false;
    }
}


class ArrayPreProcesser {
private:

    std::vector<std::vector<std::vector<int64_t>>>& _edge_to_chunk_mapping;
    const std::unordered_map<int64_t, graphar::IdType>& _vertex_prop_index_map;
    int _num_threads;
    int _chunk_size;
    int _num_of_drops; 
    std::string& _path_to_tmp;
    bool _wrote_tmp_files;

public:

    ArrayPreProcesser(std::vector<std::vector<std::vector<int64_t>>>& edge_to_chunk_mapping,
                      const std::unordered_map<int64_t, graphar::IdType>& vertex_prop_index_map,
                      int num_threads, int chunk_size, int num_of_drops, std::string& path_to_tmp)
                      : _edge_to_chunk_mapping{edge_to_chunk_mapping}, _vertex_prop_index_map{vertex_prop_index_map},
                        _num_threads{num_threads}, _chunk_size{chunk_size}, _num_of_drops{num_of_drops}, 
                        _path_to_tmp{path_to_tmp}, _wrote_tmp_files{false} {}


    bool WroteTmpFiles() { return _wrote_tmp_files; }

    // non-numeric type forbidden
    arrow::Status Visit(const arrow::Array& array) {
        return arrow::Status::NotImplemented("Unsupported type: ",
                                            array.type()->ToString());
    }

    template <typename ArrayType, typename T = typename ArrayType::TypeClass>
    arrow::enable_if_number<T, arrow::Status> Visit(const ArrayType& arr) {
        
        const auto* data = arr.raw_values();
        int64_t length = arr.length();

        int64_t batch_size = length % _num_of_drops == 0 ? length / _num_of_drops : length / _num_of_drops + 1;
        for(int64_t start = 0; start < length; start += batch_size) {
            int64_t end = std::min(start + batch_size, length);

            #pragma omp parallel for schedule(static) num_threads(_num_threads)
            for (int64_t i = start; i < end; ++i) {

                int thread_id = omp_get_thread_num();
                int64_t key = static_cast<int64_t>(data[i]);

                auto val = _vertex_prop_index_map.find(key);

                if (val == _vertex_prop_index_map.end()) {

                    #pragma omp critical
                    {
                        std::cout << "[Error: mapping] Could not find object in vertex_prop_index_map, row: " << i
                                << " value: " << key
                                << " thread: " << thread_id
                                << std::endl;
                    }
                    continue;
                }

                _edge_to_chunk_mapping[thread_id][val->second / _chunk_size].push_back(i);
            }

            if(_path_to_tmp != "") {
                _wrote_tmp_files = WriteMappingNClearVector(_edge_to_chunk_mapping, _path_to_tmp, _num_threads);
                if (_wrote_tmp_files)
                    logger("    Wrote ["+std::to_string(start)+", "+std::to_string(end)+"] mapping to '"+_path_to_tmp+"'.");
                else {
                    logger("    [ERROR] Could not write mapping to '"+_path_to_tmp+"'.");

                    // if this is the first chunk, and we have no data on disk
                    if(start == 0) {
                        _path_to_tmp = "";  // we will store everything in memory
                        logger("    Since writing to tmp folder failed, data will be stored in memory.");
                    } else {
                        clear_directory(_path_to_tmp);
                        throw std::runtime_error("Could not write files to tmp directory for batch "+std::to_string(start / batch_size + 1)+" (starting from 1). Can't recover.");
                    }
                }
            }
        }

        if(_path_to_tmp != "") {
            _edge_to_chunk_mapping.clear();
            std::vector<std::vector<std::vector<int64_t>>>().swap(_edge_to_chunk_mapping);
        }

        return arrow::Status::OK();
    }
};


bool PreProcessArray(
    const std::shared_ptr<arrow::Array>& column,
    std::vector<std::vector<std::vector<int64_t>>>& edge_to_chunk_mapping,
    const std::unordered_map<int64_t, graphar::IdType>& vertex_prop_index_map,
    int num_threads, int chunk_size, int num_of_drops, std::string& path_to_tmp)
{
    ArrayPreProcesser processor(edge_to_chunk_mapping, vertex_prop_index_map, 
                                num_threads, chunk_size, num_of_drops, path_to_tmp);
    arrow::Status st = arrow::VisitArrayInline(*column, &processor);  // TODO check
    
    return processor.WroteTmpFiles();
}

void GetVertexChunkSizes(
    std::shared_ptr<const graphar::GraphInfo> graph_info,
    std::unordered_map<std::string, graphar::IdType>& vertex_chunk_sizes) {

    for (const auto& vertex_info : graph_info->GetVertexInfos()) {
        vertex_chunk_sizes[vertex_info->GetType()] = vertex_info->GetChunkSize();
    }
}

/**
 * @brief Constructs a vector of vertex PropertyGroups, filtering out primary key properties.
 * 
 * This function iterates through the source vertex's property groups, excludes any 
 * properties marked as primary keys (is_primary=true), and creates new PropertyGroup 
 * instances. The generated groups are assigned auto-incremented names following the 
 * pattern "{vertex_type}_properties_{index}".
 * 
 * @param vertex The source MergeVertex structure containing raw property group definitions.
 * @param start_index The starting index for numbering the generated property groups. 
 *                    The function increments this value for each processed group.
 * @return std::vector<std::shared_ptr<graphar::PropertyGroup>> A vector of shared pointers 
 *         to the newly created GraphAr PropertyGroup objects.
 */
std::vector<std::shared_ptr<graphar::PropertyGroup>> BuildVertexPropertyGroupsExcludingPk(const MergeVertex& vertex, 
                                                                    int start_index) {
    std::vector<std::shared_ptr<graphar::PropertyGroup>> pgs;
    for (const auto& pg : vertex.property_groups) {
        ++start_index;
        std::vector<graphar::Property> props;
        for (const auto& prop : pg.properties) {
            if(prop.is_primary) {
                continue;
            }
            props.emplace_back(prop.name, graphar::DataType::TypeNameToDataType(prop.data_type), 
                    prop.is_primary, prop.nullable);
        }
        auto property_group = graphar::CreatePropertyGroup(
            props, graphar::StringToFileType(pg.file_type), 
            vertex.type+"_properties_"+std::to_string(start_index));
        pgs.emplace_back(property_group);
    }
    return pgs;
}


void AddPgsFromVertexInfo(std::vector<std::shared_ptr<graphar::PropertyGroup>>& pgs,
                          const MergeVertex& vertex_merge) {

    std::string primary_key;
    int number_of_pgroups = pgs.size();

    for (const auto& pg : vertex_merge.property_groups) {
        std::vector<graphar::Property> props;
        for (const auto& prop : pg.properties) {
            if (prop.is_primary) {
                if (!primary_key.empty()) {
                    throw std::runtime_error("Multiple primary keys found in vertex " +
                                            vertex_merge.type);
                }
                primary_key = prop.name;
            }
        }
    }
    auto AddedPgs = BuildVertexPropertyGroupsExcludingPk(vertex_merge, number_of_pgroups);
    pgs.insert(pgs.end(), AddedPgs.begin(), AddedPgs.end());
    logger("    Additional PG added to config.");
}


/**
 * @brief Finds the path in the graph to the folder containing the vertex's primary attribute.
 * 
 * @param vertex_info Original vertex metadata storing attribute names and their corresponding paths.
 * @param join_on Vertex attribute to be found in the original graph.
 * @return std::string Relative path to the folder with the primary property.
 * @throws std::runtime_error If the property specified as the primary key is not found in the vertex schema.
 */
std::string FindPathToProperty(std::shared_ptr<const graphar::VertexInfo> vertex_info,
                                      const std::string& join_on) {

    std::vector<std::shared_ptr<graphar::PropertyGroup>> original_pgs = vertex_info->GetPropertyGroups();
    std::shared_ptr<graphar::PropertyGroup> pg_with_user_PK;
    for(auto& pg: original_pgs) {
        for (const auto& prop : pg->GetProperties()) {
            if (prop.name == join_on) {
                pg_with_user_PK = pg;
                break;
            }
        }
    }
    if (pg_with_user_PK.get() == nullptr) {
        throw std::runtime_error("No property '"+join_on+"' found in original schema.");
    }
    return vertex_info->GetPathPrefix(pg_with_user_PK).value();
}

std::shared_ptr<arrow::Table> ReadTable(const Source& source, size_t num_threads = 1) {

    std::vector<std::string> new_column_names;
    for (const auto& [key, value] : source.columns) {
        new_column_names.emplace_back(key);
    }

    // Read source
    std::shared_ptr<arrow::Table> table;
    {
        std::vector<std::shared_ptr<arrow::Table>> file_tables(source.path.size());

        #pragma omp parallel for schedule(dynamic) num_threads(std::min(num_threads, source.path.size()))
        for (int i = 0; i < source.path.size(); ++i) {
            file_tables[i] = GetDataFromFile(source.path[i], new_column_names, source.delimiter,
                                source.file_type);
        }
        table = ConcatenateTables(file_tables).ValueOrDie();
    }

    return table;
}

std::shared_ptr<arrow::Table> ReadPropertiesFromTable(const Source& source, 
                                                      const std::vector<Property>& properties, 
                                                      const std::vector<std::string>& primary_attributes,
                                                      size_t num_threads = 1) {

    std::vector<std::string> columns_to_read{primary_attributes};
    for (const auto& prop: properties)
        columns_to_read.emplace_back(prop.name);

    std::vector<std::string> new_column_names;
    for(const auto& [data_column_name, prop_name] : source.columns) {
        for (const auto& prop: columns_to_read) {
            if (prop == prop_name) {
                new_column_names.emplace_back(data_column_name);
            }
        }
    }

    // Read source
    std::shared_ptr<arrow::Table> table;
    {
        std::vector<std::shared_ptr<arrow::Table>> file_tables(source.path.size());

        #pragma omp parallel for schedule(dynamic) num_threads(std::min(num_threads, source.path.size()))
        for (int i = 0; i < source.path.size(); ++i) {
            file_tables[i] = GetDataFromFile(source.path[i], new_column_names, source.delimiter,
                                source.file_type);
        }
        table = ConcatenateTables(file_tables).ValueOrDie();
    }

    return table;
}


std::unordered_map<std::string, 
                   std::pair<std::string, std::shared_ptr<arrow::DataType>>> CollectColumnsToChange(
                        const std::unordered_map<std::string, std::string>& source_columns,
                        const std::vector<PropertyGroup>& property_groups,
                        std::shared_ptr<const arrow::Table> table) {

    std::unordered_map<std::string, Property> column_prop_map;
    std::unordered_map<std::string, std::string> reversed_columns_config;

    for (const auto& [key, value] : source_columns) {
        reversed_columns_config[value] = key;
    }
    for (const auto& pg : property_groups) {
        for (const auto& prop : pg.properties) {
            column_prop_map[reversed_columns_config[prop.name]] = prop;
        }
    }

    std::unordered_map<
        std::string, std::pair<std::string, std::shared_ptr<arrow::DataType>>>
        columns_to_change;
    for (const auto& [column, prop] : column_prop_map) {
        auto arrow_data_type = graphar::DataType::DataTypeToArrowDataType(
            graphar::DataType::TypeNameToDataType(prop.data_type));
        auto arrow_column = table->GetColumnByName(column);

        if (!prop.nullable) {
            for (const auto& chunk : arrow_column->chunks()) {
                if (chunk->null_count() > 0) {
                throw std::runtime_error("Non-nullable column '" + column +
                                        "' has null values");
                }
            }
        }

        if (column != prop.name || arrow_column->type()->id() != arrow_data_type->id()) {
            columns_to_change[column] = std::make_pair(prop.name, arrow_data_type);
        }
    }

    return columns_to_change;
}

std::shared_ptr<arrow::Table> MakeMergedVertexTable(const MergeVertex& vertex,
                                                    size_t num_threads = 1) {

    std::vector<std::shared_ptr<arrow::Table>> vertex_tables;
    for(const Source& source : vertex.sources) {

        // Read source & cnage name and data type
        std::shared_ptr<arrow::Table> table = ReadTable(source, num_threads);
        auto columns_to_change = CollectColumnsToChange(source.columns, vertex.property_groups, table);
        table = ChangeNameAndDataType(table, columns_to_change);
        vertex_tables.push_back(table);
    }
            
    // Merge all tables with new data into a big one
    return MergeTables(vertex_tables);
}


void MergeVertexChunkwise(std::shared_ptr<arrow::Table> vertex_table,
                          const std::vector<std::shared_ptr<graphar::PropertyGroup>>& pgs,
                          const std::shared_ptr<graphar::VertexPropertyWriter> vertex_prop_writer,
                          const std::unordered_map<int64_t, graphar::IdType>& pk2row_num, 
                          const std::string& join_on,
                          const std::string& path_to_pk,
                          size_t num_threads = 1) {

    std::vector<std::string> column_names = {join_on};
    std::vector<std::filesystem::directory_entry> parts = ListDirectoryEntries(path_to_pk);

    logger("      Merging data to vertex chunks.");
    #pragma omp parallel for schedule(dynamic) num_threads(std::min(num_threads, parts.size()))
    for (int64_t i = 0; i < parts.size(); ++i) {
        auto& file = parts[i];

        // read one vertex chunk in GraphAr format
        std::shared_ptr<arrow::ChunkedArray> vertex_chunk_column = 
                        GetDataFromParquetFile(file.path().string(), column_names)->column(0);

        arrow::Int64Builder builder;
        std::optional<int> vertex_chunk_idx = extract_tailing_number(file);
        if(!vertex_chunk_idx.has_value()) {  // in case we change format and other files will appear in the same directory
            logger("  [WARNING] Found file with no tailing number in graph's vertex.");
            continue;
        }

        // for each PK find the corresponding line number in additional attributes
        CollectRowNumbers(vertex_chunk_column, builder, pk2row_num);

        // collect the result
        std::shared_ptr<arrow::Array> indices_order;
        builder.Finish(&indices_order);

        // exctract data in correct order
        arrow::compute::TakeOptions options;
        auto maybe_sorted_chunk = arrow::compute::Take(vertex_table, indices_order, options);
        auto sorted_chunk = maybe_sorted_chunk.ValueOrDie().table();

        // Write table
        for (const auto& property_group : pgs) {
            graphar::Status st = vertex_prop_writer->WriteTable(sorted_chunk, property_group,
                                            vertex_chunk_idx.value());

            if(st.IsInvalid()) { 
                throw std::runtime_error("Could not write vertex property chunk: " + st.message());
            }
        }
    }
}


void MergeVertices(const MergeConfig& merge_config, size_t num_threads = 1) {
    // 0. Vertex load
    // 1. Read GraphAr Vertex info
    // 2. Modify & rewrite this vertex info
    // 3. Collect PK+index to unordered map
    // 4. For each element in new table, get internal graphAr index-> Add data to this posotion
    // 5. Save

    // 0. prepare to merge
    auto graph_info = graphar::GraphInfo::Load(
            merge_config.graphar_config.path+"/"+merge_config.graphar_config.name+".yaml").value();
    auto version = graphar::InfoVersion::Parse(merge_config.graphar_config.version).value();

    std::unordered_map<std::string, graphar::IdType> vertex_chunk_sizes;
    GetVertexChunkSizes(graph_info, vertex_chunk_sizes);

    logger("Processing vertices");
    for (const auto& vertex : merge_config.merge_schema.vertices) {

        // 1.1 Go to the graph description yml's and load information about this vertex
        logger("  Processing vertex <"+vertex.type+">.");
        auto vertex_info = graph_info->GetVertexInfo(vertex.type);

        // 1.2 Read info about property groups that will be added and add it to the current information
        // TODO: note: this looks a lot like importer.h, we probably need refactoring 
        logger("    Reading PG that should be added.");

        std::vector<std::shared_ptr<graphar::PropertyGroup>> pgs = vertex_info->GetPropertyGroups();
        AddPgsFromVertexInfo(pgs, vertex);

        // Update vertex info
        auto vertex_info_updated =
                    graphar::CreateVertexInfo(vertex.type, vertex.chunk_size, pgs,
                                  vertex.labels, vertex.prefix, version);

        auto file_name = vertex.type + ".vertex.yaml";
        auto save_path = merge_config.graphar_config.path;
        {
            graphar::Status st = vertex_info_updated->Save(save_path + file_name);
            if(st.IsInvalid()) {
                throw std::runtime_error("Could not write vertex info: " + st.message());
            }
        }
        
        logger("    Saved updated vertex description.");

        // Create vertex property writer to save new data
        save_path += "/";
        auto vertex_prop_writer = graphar::VertexPropertyWriter::Make(
                                    vertex_info_updated, save_path,
                                    StringToValidateLevel(vertex.validate_level))
                                    .value();

        // 1.3 Read graph vertices' columns with PK and graphar index
        // 1.3.1 Read graph's original PG to find user's PK there
        std::string path_original = merge_config.graphar_config.path + '/' + FindPathToProperty(vertex_info, vertex.join_on);
        logger("    Looking for original data in "+path_original);

        // 1.3.3 Read new data
        std::shared_ptr<arrow::Table> merged_vertex_table = MakeMergedVertexTable(vertex, num_threads = 1);

        // 1.3.4 Save map[user_pk] = row-number-in-input-table
        // note: only int64/int32 keys are allowed
        logger("    Mapping PK from new data to its row in new data."); // TODO: OMP (22-01-01 6 mins one thread) ??? 
        std::unordered_map<int64_t, graphar::IdType> pk2row_num;
        auto pk_column = merged_vertex_table->GetColumnByName(vertex.join_on);
        if (pk_column->null_count() > 0) {
            throw std::runtime_error("Vertex PK property column '" + vertex.join_on + "' has NULL values.");
        }

        MapPK2row(pk_column, pk2row_num);

        // 1.3.5 For each chunk in GraphAr collect rows in additional data that match it

        MergeVertexChunkwise(merged_vertex_table, BuildVertexPropertyGroupsExcludingPk(vertex, vertex_info->GetPropertyGroups().size()), 
                             vertex_prop_writer, pk2row_num, vertex.join_on, path_original, num_threads);
        logger("  Processed vertex <"+vertex.type+">.");
    }
}

std::unordered_map<std::string, std::set<std::string>> CollectVertexPropsInEdges(const std::vector<Edge>& edges) {
    std::unordered_map<std::string, std::set<std::string>>
      vertex_props_in_edges;
    for (const auto& edge : edges) {
        vertex_props_in_edges[edge.src_type].insert(edge.src_prop);
        vertex_props_in_edges[edge.dst_type].insert(edge.dst_prop);
    }
    return vertex_props_in_edges;
}


void PreparePropertyToIndexMap(const graphar::VertexInfoVector& vertex_infos,
                            const std::unordered_map<std::string, std::set<std::string>>& vertex_props_in_edges,
                            std::map<std::pair<std::string, std::string>, std::unordered_map<int64_t, graphar::IdType>>& vertex_prop_index_map,
                            const std::string& path_to_graph) {

    for(auto vertex : vertex_infos) {
        if (vertex_props_in_edges.find(vertex->GetType()) == vertex_props_in_edges.end()) 
            continue;
        
        for (const auto& vertex_prop : vertex_props_in_edges.at(vertex->GetType())) {
            if (vertex_prop_index_map.find(std::make_pair(vertex->GetType(), vertex_prop)) != vertex_prop_index_map.end())
                continue;
            
            // find PG that contains this property
            std::optional<std::string> path_to_pg;
            for(auto& pg : vertex->GetPropertyGroups()) {
                if (pg->HasProperty(vertex_prop)) {
                    path_to_pg = pg->GetPrefix();
                    break;
                }
            } 
            if(!path_to_pg.has_value()) {
                throw std::runtime_error("No vertex property "+vertex_prop+" found in graph.");
            }
            
            std::string path_to_graphar_pg = path_to_graph + '/' + 
                                                vertex->GetPrefix() + path_to_pg.value();
            logger("  Looking for property '"+ vertex_prop + "' in " + path_to_graphar_pg);

            int64_t vertex_num = 0;
            {
                std::string path_to_vertex_count = path_to_graph + '/' + vertex->GetVerticesNumFilePath().value();
                logger("  Path to vertex num: "+path_to_vertex_count);
                std::ifstream file(path_to_vertex_count, std::ios::binary);
                file.read(reinterpret_cast<char*>(&vertex_num), sizeof(vertex_num));
                vertex_num = le64toh(vertex_num);  // TODO: important: make it correct for writer  
            }

            // read tables from directory and save property_value -> vertex_id relation
            std::unordered_map<int64_t, graphar::IdType> property_to_id_map(vertex_num);
            std::vector<std::string> column_names = {vertex_prop, graphar::GeneralParams::kVertexIndexCol};
            
            for (const auto& file : std::filesystem::directory_iterator(path_to_graphar_pg)) {  // TODO: omp (8 minutes on 22-01-01)
                std::shared_ptr<arrow::Table> vertex_chunk_prop_columns =                    
                                GetDataFromParquetFile(file.path().string(), column_names)->CombineChunks().ValueOrDie();

                auto key_col_ptr = vertex_chunk_prop_columns->GetColumnByName(vertex_prop);
                auto val_col_ptr = vertex_chunk_prop_columns->GetColumnByName(graphar::GeneralParams::kVertexIndexCol);

                if (!key_col_ptr) {
                    throw std::runtime_error("key column '" + vertex_prop + "' not found in table");
                }
                if (!val_col_ptr) {
                    throw std::runtime_error("value column '" + std::string(graphar::GeneralParams::kVertexIndexCol) + "' not found in table");
                }
      
                MapValues(key_col_ptr, val_col_ptr, property_to_id_map);
            }
            logger("  Property '" + vertex_prop + "' mapping to GraphAr id saved.");   // TODO: pause for 6 mins after that, why?
            // save map for future usage
            vertex_prop_index_map[std::make_pair(vertex->GetType(), vertex_prop)] = property_to_id_map;
        }
    }
}


std::shared_ptr<graphar::EdgeInfo> CreateUpdatedEdgeInfo(const std::shared_ptr<graphar::EdgeInfo> edge_info,
                                                         const Edge& edge,
                                                         const std::unordered_map<std::string, graphar::IdType>& vertex_chunk_sizes,
                                                         const std::shared_ptr<const graphar::InfoVersion> version) {

    auto pgs = std::vector<std::shared_ptr<graphar::PropertyGroup>>(edge_info->GetPropertyGroups());
    int number_of_pgroups = pgs.size();

    for (const auto& pg : edge.property_groups) {
        ++number_of_pgroups;
        std::vector<graphar::Property> props;
        for (const auto& prop : pg.properties) {
            graphar::Property property(
                prop.name, graphar::DataType::TypeNameToDataType(prop.data_type),
                prop.is_primary, prop.nullable);
            props.push_back(property);
        }
        auto property_group = graphar::CreatePropertyGroup(
            props, graphar::StringToFileType(pg.file_type), 
            edge.edge_type+"_properties_"+std::to_string(number_of_pgroups));
        pgs.emplace_back(property_group);
    }

    // collect adj lists info
    graphar::AdjacentListVector original_adj_lists;
    for (const auto& adj_list : edge.adj_lists) {
        original_adj_lists.push_back(graphar::CreateAdjacentList(
                                        graphar::OrderedAlignedToAdjListType(adj_list.ordered,
                                                                            adj_list.aligned_by),
                                        graphar::StringToFileType(adj_list.file_type)));
    }

    // update edge info
    return graphar::CreateEdgeInfo(
                        edge.src_type, edge.edge_type, edge.dst_type, edge.chunk_size,
                        vertex_chunk_sizes.at(edge.src_type), vertex_chunk_sizes.at(edge.dst_type),
                        true, original_adj_lists, pgs, edge.prefix, version);
} 

Source GetSourceContainingAllProperties(const std::vector<Source>& sources,
                                        const std::vector<Property>& properties) {
    std::optional<Source> source_PG;
    for (const auto& source : sources) {

        // collect properties that are defined in this source 
        std::vector<std::string> prop_names_in_source;
        prop_names_in_source.reserve(source.columns.size());
        for (const auto& [data_column_name, prop_name] : source.columns) {
            prop_names_in_source.push_back(prop_name);
        }

        // make sure all properties are in this source
        bool all_props_in_source = true;
        for(const auto& prop : properties) {
            auto it = std::find(prop_names_in_source.begin(), 
                                prop_names_in_source.end(), 
                                prop.name);
            if (it == prop_names_in_source.end()) {
                all_props_in_source = false;
                break;
            }
        }

        if(all_props_in_source) {
            source_PG = source;
            break;
        }
    }
    if(!source_PG.has_value()) {
        throw std::runtime_error("There is no source that contains all properties from this PG.");
    }

    return source_PG.value();
}

std::shared_ptr<arrow::ChunkedArray> GetPrimaryIndexColumn(const std::string& name, 
                                                           const std::shared_ptr<const arrow::Table> table) {
    std::shared_ptr<arrow::ChunkedArray> column_tmp = table->GetColumnByName(name);

    auto result = arrow::Concatenate(column_tmp->chunks());
    if (!result.ok()) {
        std::cerr << result.status().ToString() << std::endl;
        throw std::runtime_error("Could not combine chunks for PK column 1.");
    }
    auto combined_array = result.ValueOrDie();
    auto column = std::make_shared<arrow::ChunkedArray>(combined_array);

    if (column->null_count() > 0) {
        throw std::runtime_error("Edge PK property column '" + name + "' has NULL values.");
    }

    return column;
}


bool MapEdgeToChunk(std::vector<std::vector<std::vector<int64_t>>>& edge_to_chunk_mapping,
                    const std::shared_ptr<const arrow::ChunkedArray> column,
                    const std::unordered_map<int64_t, graphar::IdType>& vertex_prop_index_map,
                    int64_t edge_chunk_size, int64_t vertex_chunk_size,
                    std::string& tmp_path,
                    size_t num_threads = 1, int scale = 5, int num_of_drops = 2) {

    int num_of_chunks = 0;
    bool aligned_by_src = true;
    num_of_chunks = vertex_prop_index_map.size() / vertex_chunk_size + 1;

    for (int t = 0; t < num_threads; ++t) {
        for (int c = 0; c < num_of_chunks; ++c) {                               
            edge_to_chunk_mapping[t][c].reserve(edge_chunk_size * scale / num_threads);  // graph_info->GetEdgeInfos()[0]->GetChunkSize() 
        } // WARNING: depends on the graph, better choose constant manually for each launch
    }

    // use importer approach
    bool wrote_tmp_files = false;
    logger("    Mapping edge row to its chunk in "+std::to_string(num_threads)+" threads.");
    wrote_tmp_files = PreProcessArray(
                            column->chunk(0), edge_to_chunk_mapping,
                            vertex_prop_index_map, num_threads, vertex_chunk_size, 
                            num_of_drops, tmp_path);

    if(wrote_tmp_files) {
        edge_to_chunk_mapping = {};
    }

    return wrote_tmp_files;
}

struct EdgePropertyWriterSettings {
    const std::shared_ptr<graphar::PropertyGroup> property_group;
    const graphar::ValidateLevel validate_level;
    int chunk_idx;
};

arrow::Status BuildEdgeChunk(const std::filesystem::directory_entry& edge_chunk_path,
                             const graphar::AdjListType& adj_list_type,
                             const std::vector<EdgeSmall>& new_chunk_edges, 
                             const std::shared_ptr<arrow::Table> pg_data_table,
                             const EdgePropertyWriterSettings& settings,
                             const graphar::EdgeChunkWriter& edge_writer) {

    std::vector<std::string> column_names = 
                {graphar::GeneralParams::kSrcIndexCol, graphar::GeneralParams::kDstIndexCol};

    int wrote_succesfully = 0;

    for (const auto& chunk : std::filesystem::directory_iterator(edge_chunk_path.path())) {
        arrow::Int64Builder builder;
        std::shared_ptr<arrow::Int64Array> src_column, dst_column;
        {
            std::shared_ptr<arrow::Table> tmp_table = GetDataFromParquetFile(chunk.path().string(), column_names)
                                                        ->CombineChunks().ValueOrDie();
            src_column = std::static_pointer_cast<arrow::Int64Array>(tmp_table->GetColumnByName(column_names[0])->chunk(0));
            dst_column = std::static_pointer_cast<arrow::Int64Array>(tmp_table->GetColumnByName(column_names[1])->chunk(0));
        }

        // for each edge find reference to its additional properties
        const auto* src_column_raw = src_column->raw_values();
        const auto* dst_column_raw = dst_column->raw_values();

        // construct builder
        if (adj_list_type == graphar::AdjListType::unordered_by_dest || adj_list_type == graphar::AdjListType::unordered_by_source) {
            // when adj_lists are unordered, we will have to do binsearch on ordered by src user edges for each adj_lists edge
            ConstructBuilderBinsearch(builder, src_column_raw, dst_column_raw, new_chunk_edges, src_column->length());
        } else {
            // when adj_lists are ordered by src/dst, we can order our edges by src/dst and 'merge' them for O(n)
            ConstructBuilderLinear(builder, src_column_raw, dst_column_raw, new_chunk_edges, src_column->length(), 
                                    adj_list_type == graphar::AdjListType::ordered_by_source ? true : false);
        }

        src_column.reset();
        dst_column.reset();

        // save order of data for this edge chunk
        std::shared_ptr<arrow::Array> indices_order;
        builder.Finish(&indices_order);

        // exctract edges in correct order
        // we extract all data, but write only properties in edge order, this is why it works
        // we never replace PKs in new data with indices that we caclulated, bc we already have adj_lists 
        arrow::compute::TakeOptions options = arrow::compute::TakeOptions::NoBoundsCheck();
        auto sorted_chunk = arrow::compute::Take(pg_data_table, indices_order, options).ValueOrDie().table();

        // write them down
        std::optional<int> chunk_tailing_number = extract_tailing_number(chunk);
        if (!chunk_tailing_number.has_value()) {
            logger("  [WARNING] Found file in edge chunk with no tailing number.");
            continue;
        }
        auto status = edge_writer.WritePropertyChunk(sorted_chunk, settings.property_group, settings.chunk_idx, 
                                                        chunk_tailing_number.value(), settings.validate_level);

        if(!status.ok()) {
            logger("[ERROR] Could not write chunk: " + status.message());
        } else {
            ++wrote_succesfully;
        }
    }

    if (wrote_succesfully)
        return arrow::Status::OK();  // TODO stop in case of mistake
    return arrow::Status::IOError("Cound not write any chunk for this edge part");
}

void BuildEdge(const std::string& path_to_adjlist,
                const std::string& tmp_path,
                bool wrote_tmp_files, 
                std::vector<std::vector<std::vector<int64_t>>>& edge_to_chunk_mapping,
                const std::shared_ptr<const arrow::ChunkedArray> src_column,
                const std::shared_ptr<const arrow::ChunkedArray> dst_column,
                const Edge& edge, const graphar::AdjListType& adj_list_type,
                std::shared_ptr<graphar::PropertyGroup> property_group, 
                const std::shared_ptr<arrow::Table> pg_data_table,
                const graphar::EdgeChunkWriter& edge_writer,
                const std::map<std::pair<std::string, std::string>, std::unordered_map<int64_t, graphar::IdType>>& vertex_prop_index_map,
                size_t num_threads = 1) {

    std::vector<std::filesystem::directory_entry> parts = ListDirectoryEntries(path_to_adjlist);
    int processed_chunks = 0;

    #pragma omp parallel for schedule(dynamic) num_threads(std::min(num_threads, parts.size()))
    for (int64_t i = 0; i < parts.size(); ++i) {

        auto& edge_chunk_path = parts[i];
        std::optional<int> edge_chunk_idx = extract_tailing_number(edge_chunk_path);
        if(!edge_chunk_idx.has_value()) {
            logger("  [WARNING] Found edge chunk with no tailing number.");
            continue;
        }
        std::string path_to_mapping = make_mapping_path(tmp_path, edge_chunk_idx.value());

        // calculate number of edges
        int64_t num_of_edges_in_chunk = 0;
        if(!wrote_tmp_files) {
            for(int thread = 0; thread < edge_to_chunk_mapping.size(); ++thread) {
                num_of_edges_in_chunk += edge_to_chunk_mapping[thread][edge_chunk_idx.value()].size();
            }
        } else {
            num_of_edges_in_chunk = get_count_bin(path_to_mapping);
        }

        // collect edges by edge_chunk_idx, sort them
        std::vector<EdgeSmall> new_chunk_edges;
        if(wrote_tmp_files) {
            with_streamer(path_to_mapping, edge_chunk_idx.value(),
                [&](auto& s) {
                    ExtractEdges<arrow::Int64Array, arrow::Int64Array>(
                        s, src_column, dst_column, 
                        vertex_prop_index_map.at(std::make_pair(edge.src_type, edge.src_prop)),
                        vertex_prop_index_map.at(std::make_pair(edge.dst_type, edge.dst_prop)),
                        num_of_edges_in_chunk, new_chunk_edges
                    );
                });
        } else {
            with_streamer(edge_to_chunk_mapping, edge_chunk_idx.value(),
                [&](auto& s) {
                    ExtractEdges<arrow::Int64Array, arrow::Int64Array>(
                        s, src_column, dst_column, 
                        vertex_prop_index_map.at(std::make_pair(edge.src_type, edge.src_prop)),
                        vertex_prop_index_map.at(std::make_pair(edge.dst_type, edge.dst_prop)),
                        num_of_edges_in_chunk, new_chunk_edges
                    );
                });
        }


        // Case 1: ordered by src/dst -> we can use linear search, if we sort edges by src/dst
        // Case 2: unordered -> we must use binsearch, so sort everything by src
        if (adj_list_type != graphar::AdjListType::ordered_by_dest) {
            std::sort(new_chunk_edges.begin(), new_chunk_edges.end(),
                    [](const EdgeSmall& a, const EdgeSmall& b){return a.src == b.src ? a.dst < b.dst : a.src < b.src;});
        } else {
            std::sort(new_chunk_edges.begin(), new_chunk_edges.end(),
                    [](const EdgeSmall& a, const EdgeSmall& b){return a.dst == b.dst ? a.src < b.src : a.dst < b.dst;});
        }

        // read one edge chunk and make builder for it
        EdgePropertyWriterSettings settings{property_group, 
                                            StringToValidateLevel(edge.validate_level),
                                            edge_chunk_idx.value()};
        auto st = BuildEdgeChunk(edge_chunk_path, adj_list_type, new_chunk_edges, 
                                pg_data_table, settings, edge_writer);
        if(!st.ok()) {
            logger("[ERROR] Could not write chunk: " + st.message());
            throw std::runtime_error("Chunk building error.");
        }

        #pragma omp critical
        {
            processed_chunks += 1;
            logger("      Processed "+std::to_string(processed_chunks)+"/" + std::to_string(parts.size()) + " edge chunks.");
        }
    }
}

void MergeEdges(MergeConfig& merge_config,
                const std::shared_ptr<const graphar::GraphInfo> graph_info,
                const std::map<std::pair<std::string, std::string>, 
                        std::unordered_map<int64_t, graphar::IdType>>& vertex_prop_index_map,
                size_t num_threads = 1) {

    std::unordered_map<std::string, graphar::IdType> vertex_chunk_sizes;
    GetVertexChunkSizes(graph_info, vertex_chunk_sizes);

    auto version = graphar::InfoVersion::Parse(merge_config.graphar_config.version).value();
    fs::path save_path = merge_config.graphar_config.path;

    // 1. Work with one edge at a time
    for (const auto& edge : merge_config.merge_schema.edges) {
        logger("  Processing edge <"+edge.edge_type+">.");

        // 1.2 Create edge_info & edge_writer for this edge
        auto edge_info = graph_info->GetEdgeInfo(edge.src_type, edge.edge_type, edge.dst_type);

        // collect all pgs that will be added to this edge
        auto updated_edge_info = CreateUpdatedEdgeInfo(edge_info, edge, vertex_chunk_sizes, version);

        // Work with one new PG at a time
        // TODO: additional properties already exist -> overwrite them, add overwrite flg in config ???
        for(auto& pg : edge.property_groups) {

            // 1.3.1 Define which source has this PG data
            Source source_PG = GetSourceContainingAllProperties(edge.sources, pg.properties);

            // 1.3.2 Read source table with new PG
            std::shared_ptr<arrow::Table> pg_data_table = ReadPropertiesFromTable(source_PG, pg.properties, 
                                                                                  {edge.src_edge_prop, edge.dst_edge_prop}, num_threads);
            logger("    PG source read: "+std::to_string(source_PG.path.size()) +" tables concatenated.");
            
            std::unordered_map<std::string, std::string> reversed_columns;
            for (const auto& [key, value] : source_PG.columns) {
                reversed_columns[value] = key;
            }

            // Change name and data type
            std::unordered_map<
                std::string,
                std::pair<std::string, std::shared_ptr<arrow::DataType>>>
                columns_to_change = CollectColumnsToChange(source_PG.columns, edge.property_groups, pg_data_table);
            pg_data_table = ChangeNameAndDataType(pg_data_table, columns_to_change);
            logger("    Name & data type changed, columns to change: "+std::to_string(columns_to_change.size()));
            pg_data_table = pg_data_table->CombineChunks().ValueOrDie();

            // 1.3.3 Get columns with src&dst
            auto src_column = GetPrimaryIndexColumn(reversed_columns[edge.src_edge_prop], pg_data_table);
            auto dst_column = GetPrimaryIndexColumn(reversed_columns[edge.dst_edge_prop], pg_data_table);

            for(auto column_to_remove: std::vector<std::string>{reversed_columns[edge.src_edge_prop], reversed_columns[edge.dst_edge_prop]}) {
                logger("    Removing column " + column_to_remove);
                int remove_idx = pg_data_table->schema()->GetFieldIndex(column_to_remove);
                pg_data_table = pg_data_table->RemoveColumn(remove_idx).ValueOrDie();
            }

            // 1.3.4 Work with each adj_lists type required by user
            for (const auto& adj_list : edge.adj_lists) {  // TODO: user demands adj_list that does not exist in original graph
                logger("    Working with adj_list aligned by "+adj_list.aligned_by);
                num_threads = omp_get_max_threads() / 2;

                auto adj_lst = graphar::CreateAdjacentList(
                                    graphar::OrderedAlignedToAdjListType(adj_list.ordered,
                                                                        adj_list.aligned_by),
                                    graphar::StringToFileType(adj_list.file_type)
                );

                // create writer for this edge & adj list type
                graphar::EdgeChunkWriter edge_writer(updated_edge_info, save_path.string()+"/", adj_lst->GetType(), 
                                                     graphar::WriterOptions::DefaultWriterOption(),
                                                     StringToValidateLevel(edge.validate_level));
                
                // calculate number of chunks according to the number of src/dst vertices
                int num_of_chunks = 0;
                int64_t vertex_chunk_size = 0;
                bool aligned_by_src = true;

                if(adj_lst->GetType() == graphar::AdjListType::ordered_by_source ||
                   adj_lst->GetType() == graphar::AdjListType::unordered_by_source) {
                    vertex_chunk_size = vertex_chunk_sizes[edge.src_type];
                    num_of_chunks = vertex_prop_index_map.at(std::make_pair(edge.src_type, edge.src_prop)).size() / 
                                    vertex_chunk_size + 1;
                } else {
                    aligned_by_src = false;
                    vertex_chunk_size = vertex_chunk_sizes[edge.dst_type];
                    num_of_chunks = vertex_prop_index_map.at(std::make_pair(edge.dst_type, edge.dst_prop)).size() / 
                                    vertex_chunk_size + 1;
                }

                // map edge row to its chunk
                std::vector<std::vector<std::vector<int64_t>>> edge_to_chunk_mapping(
                    num_threads,
                    std::vector<std::vector<int64_t>>(num_of_chunks)
                );
                bool wrote_tmp_files = false;
                logger("    Mapping edge row to its chunk in "+std::to_string(num_threads)+" threads.");
                if (adj_lst->GetType() == graphar::AdjListType::ordered_by_source ||
                    adj_lst->GetType() == graphar::AdjListType::unordered_by_source)
                {
                    wrote_tmp_files = MapEdgeToChunk(edge_to_chunk_mapping, src_column, 
                                            vertex_prop_index_map.at(std::make_pair(edge.src_type, edge.src_prop)),
                                            graph_info->GetEdgeInfos()[0]->GetChunkSize(), edge_info->GetSrcChunkSize(),  // TODO: vertex cgunk size ???
                                            merge_config.tmp_path, num_threads);
                } else {
                    wrote_tmp_files = MapEdgeToChunk(edge_to_chunk_mapping, dst_column, 
                                            vertex_prop_index_map.at(std::make_pair(edge.dst_type, edge.dst_prop)),
                                            graph_info->GetEdgeInfos()[0]->GetChunkSize(), edge_info->GetDstChunkSize(),
                                            merge_config.tmp_path, num_threads);
                }
                logger("    Mapping complete.");

                // Sort edges the same way they are sorted in chunks
                std::string path_to_adjlist = merge_config.graphar_config.path + '/' 
                                              + edge.prefix + adj_lst->GetPrefix()+"adj_list";
                logger("    Looking for original data in "+path_to_adjlist);

                num_threads = omp_get_max_threads() / 4;
                logger("    Building edges in " + std::to_string(num_threads) + " threads.");
                BuildEdge(path_to_adjlist, merge_config.tmp_path, wrote_tmp_files, edge_to_chunk_mapping,
                            src_column, dst_column, edge, adj_lst->GetType(), 
                            updated_edge_info->GetPropertyGroup(pg.properties[0].name),
                            pg_data_table, edge_writer, vertex_prop_index_map, num_threads);
            }
        }

        // 1.3.5 write new edge description
        auto file_name = edge.src_type + "_" + edge.edge_type + "_" + edge.dst_type + ".edge.yaml";
        auto status = updated_edge_info->Save(save_path / file_name);
        if(!status.ok()) {
            logger("[ERROR] Could not write edge description file: " + status.message());
        }
    }
}

} // namespace

std::string DoMerge(const py::dict& config_dict)
{
    logger("Mege started");
    size_t num_threads = omp_get_max_threads() / 2;

    // get config data
    MergeConfig merge_config;
    merge_config.fill(config_dict);
    auto graph_info = graphar::GraphInfo::Load(
            merge_config.graphar_config.path+"/"+merge_config.graphar_config.name+".yaml").value();

    // 1. Add attributes to vertices
    logger("Processing vertices");
    MergeVertices(merge_config, num_threads);

    // 2. Prepare to add attributes to edges
    logger("Processing edges.");

    // 2.1. We should know graphar ids of vertices to which we refer in edges
    std::map<std::pair<std::string, std::string>, 
           std::unordered_map<int64_t, graphar::IdType>> vertex_prop_index_map;
    std::unordered_map<std::string, std::set<std::string>>
      vertex_props_in_edges = CollectVertexPropsInEdges(merge_config.merge_schema.edges);

    // 2.1.2 For each vertex type used in edges, find properties which
    //       edges refer to, read property & id columns and save property->id
    //       relation in the unordered_map.
    PreparePropertyToIndexMap(graph_info->GetVertexInfos(), vertex_props_in_edges, 
                                vertex_prop_index_map, merge_config.graphar_config.path);

    // 3. Add attributes to edges
    MergeEdges(merge_config, graph_info, vertex_prop_index_map, num_threads);

    return "Merged successfully!";
}

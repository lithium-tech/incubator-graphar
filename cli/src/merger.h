#pragma once

#include <iostream>
#include <filesystem>
#include <pybind11/pybind11.h>
#include "pybind11/stl.h"
#include "util.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

/**
 * Этот комментарий нужен для записывания мгновенных идей.
 * По поводу вершин: по пользовательскому(!) ПК мы можем
 * определить внутренний id графаря, а по внутреннему id
 * можно посчитать чанк и даже позицию в нем!
 */

namespace py = pybind11;

std::string DoMerge(const py::dict& config_dict)
{
    logger("Mege started");

    // getting config data
    MergeConfig merge_config;
    merge_config.fill(config_dict);
    auto graph_info = graphar::GraphInfo::Load(
            merge_config.graphar_config.path+"/"+merge_config.graphar_config.name+".yaml").value();

    // Create some usefull containers and read useful values
    fs::path save_path = merge_config.graphar_config.path;
    graphar::VertexInfoVector vertices_info;
    auto version = graphar::InfoVersion::Parse(merge_config.graphar_config.version).value();

    // 0. Vertex load
    // 1. Read GraphAr Vertex info
    // 2. Modify & rewrite this vertex info
    // 3. Collect PK+index to unordered map
    // 4. For each element in new table, get internal graphAr index-> Add data to this posotion
    // 5. Dump

    // 1. Add attributes to vertices
    logger("Processing vertices");
    for (const auto& vertex : merge_config.merge_schema.vertices) {

        // 1.1 Go to the graph description yml's and load information about this vertex
        logger("Processing vertex <"+vertex.type+">.");
        auto vertex_info = graph_info->GetVertexInfo(vertex.type);

        // 1.2 Read info about property groups that will be added and add to the current information
        // TODO: note: this looks a lot like importer.h, we probably need refactoring 
        logger("  Reading PG that should be added.");
        std::string primary_key;
        auto pgs = std::vector<std::shared_ptr<graphar::PropertyGroup>>(vertex_info->GetPropertyGroups());
        int number_of_pgroups = pgs.size();

        for (const auto& pg : vertex.property_groups) {
            ++number_of_pgroups;
            std::vector<graphar::Property> props;
            for (const auto& prop : pg.properties) {
                if (prop.is_primary) {
                    if (!primary_key.empty()) {
                        throw std::runtime_error("Multiple primary keys found in vertex " +
                                                vertex.type);
                    }
                    primary_key = prop.name;
                } else {
                    graphar::Property property(
                        prop.name, graphar::DataType::TypeNameToDataType(prop.data_type),
                        prop.is_primary, prop.nullable);
                    props.push_back(property);
                }
            }
            auto property_group = graphar::CreatePropertyGroup(
                props, graphar::StringToFileType(pg.file_type), 
                vertex.type+"_properties_"+std::to_string(number_of_pgroups));
            pgs.emplace_back(property_group);
        }
        logger("  Additional PG added to config.");

        // Update vertex info
        auto vertex_info_updated =
                    graphar::CreateVertexInfo(vertex.type, vertex.chunk_size, pgs,
                                  vertex.labels, vertex.prefix, version);

        auto file_name = vertex.type + ".vertex.yaml";
        auto res = vertex_info_updated->Save(save_path / file_name);
        vertices_info.push_back(vertex_info_updated);
        logger("  Saved updated vertex description.");

        // Create vertex property writer to save new data
        auto save_path_str = save_path.string();
        save_path_str += "/";
        auto vertex_prop_writer = graphar::VertexPropertyWriter::Make(
                                    vertex_info_updated, save_path_str,
                                    StringToValidateLevel(vertex.validate_level))
                                    .value();

        // 1.3 Read graph's vertices' columns with PK and graphar index
        // 1.3.1 Read graph's original PG to find user's PK there
        std::vector<std::shared_ptr<graphar::PropertyGroup>> original_pgs = vertex_info->GetPropertyGroups();
        std::shared_ptr<graphar::PropertyGroup> pg_with_user_PK;
        for(auto& pg: original_pgs) {
            for (const auto& prop : pg->GetProperties()) {
                if (prop.name == vertex.join_on) {
                    pg_with_user_PK = pg;
                    break;
                }
            }
        }
        if (pg_with_user_PK.get() == nullptr) {
            throw std::runtime_error("No property '"+vertex.join_on+"' found in original schema.");
        }
        std::string path_original = merge_config.graphar_config.path + '/' + 
                                    vertex_info->GetPathPrefix(pg_with_user_PK).value();
        logger("  Looking for original data in "+path_original);

        // 1.3.2 Read graph's PG that contains user's PK
        std::shared_ptr<arrow::Table> table;
        {
            std::vector<std::string> column_names = {graphar::GeneralParams::kVertexIndexCol, vertex.join_on};
            std::vector<std::shared_ptr<arrow::Table>> file_tables;
            for (const auto& file : std::filesystem::directory_iterator(path_original)) {
                 // We should change that if we want graphar not only in parquet
                file_tables.push_back(GetDataFromParquetFile(file.path().string(), column_names));
            }
            table = ConcatenateTables(file_tables).ValueOrDie();
            logger("  Original vertices read.");
        }

        // 1.3.3 Save map[user_pk] = graphar_index
        std::unordered_map<std::shared_ptr<arrow::Scalar>, graphar::IdType,
                   arrow::Scalar::Hash, arrow::Scalar::PtrsEqual> pk2index = TableToUnorderedMap(
                    table, vertex.join_on, graphar::GeneralParams::kVertexIndexCol
                );
        logger("  Map created.");

        // 1.3.4 Read new data batch-wise, put it into right table
        for(Source source : vertex.sources) {
            // Read source's column names
            std::vector<std::string> new_column_names;
            for (const auto& [key, value] : source.columns) {
                new_column_names.emplace_back(key);
            }

            // Read source batch-wise
            for (int i = 0; i < source.path.size(); ++i) {
                std::shared_ptr<arrow::RecordBatchReader> batch_reader = 
                    GetDataAsBatch(source.path[i], new_column_names, source.delimiter, source.file_type);
            }
        }
    }

    return "Merged successfully!";
}


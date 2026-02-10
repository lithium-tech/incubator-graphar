#pragma once

#include <iostream>
#include <filesystem>
#include <pybind11/pybind11.h>
#include "pybind11/stl.h"

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

    // 1. Add info to vertices
    logger("Processing vertices");
    for (const auto& vertex : merge_config.merge_schema.vertices) {

        // Go to the graph description yml's and load information about this vertex
        logger("Processing vertex <"+vertex.type+">.");
        auto vertex_info = graph_info->GetVertexInfo(vertex.type);

        // Read info about property groups that will be added
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
    }

    return "Merged successfully!";
}


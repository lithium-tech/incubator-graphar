/*TODO: description*/

#pragma once

#include "dataclasses.h"

#include <iostream>
#include <fstream>

void logger(std::string message)
{
        std::ifstream statm("/proc/self/statm");
        size_t size, resident;
        statm >> size >> resident;
        std::cout << "[" << resident*getpagesize()/(1024) << " Kb] " << message
                  << std::endl;
}

namespace py = pybind11;
namespace fs = std::filesystem;

void GraphArConfig::fill(const py::dict& config_dict) {
  path = config_dict["path"].cast<std::string>();
  name = config_dict["name"].cast<std::string>();
  version = config_dict["version"].cast<std::string>();
}

void Property::fill(const py::dict& config_dict) {
  name = config_dict["name"].cast<std::string>();
  data_type = config_dict["data_type"].cast<std::string>();
  is_primary = config_dict["is_primary"].cast<bool>();
  nullable = config_dict["nullable"].cast<bool>();
}

void PropertyGroup::fill(const py::dict& config_dict) {
  file_type = config_dict["file_type"].cast<std::string>();
  auto prop_list = config_dict["properties"].cast<std::vector<py::dict>>();
  for (const auto& prop_dict : prop_list) {
    Property prop;
    prop.fill(prop_dict);
    properties.emplace_back(prop);
  }
}

ImportConfig ConvertPyDictToConfig(const py::dict& config_dict) {
  ImportConfig import_config;

  auto graphar_dict = config_dict["graphar"].cast<py::dict>();
  import_config.graphar_config.fill(graphar_dict);

  auto schema_dict = config_dict["import_schema"].cast<py::dict>();

  auto vertices_list = schema_dict["vertices"].cast<std::vector<py::dict>>();
  for (const auto& vertex_dict : vertices_list) {
    Vertex vertex;
    vertex.type = vertex_dict["type"].cast<std::string>();
    vertex.chunk_size = vertex_dict["chunk_size"].cast<int>();
    vertex.prefix = vertex_dict["prefix"].cast<std::string>();
    vertex.validate_level = vertex_dict["validate_level"].cast<std::string>();
    vertex.labels = vertex_dict["labels"].cast<std::vector<std::string>>();

    auto pg_list = vertex_dict["property_groups"].cast<std::vector<py::dict>>();
    for (const auto& pg_dict : pg_list) {
      PropertyGroup pg;
      pg.fill(pg_dict);
      vertex.property_groups.emplace_back(pg);
    }

    auto source_list = vertex_dict["sources"].cast<std::vector<py::dict>>();
    for (const auto& source_dict : source_list) {
      Source src;
      src.file_type = source_dict["file_type"].cast<std::string>();
      src.path = source_dict["path"].cast<std::vector<std::string>>();
      src.delimiter = source_dict["delimiter"].cast<char>();
      src.columns = source_dict["columns"]
                        .cast<std::unordered_map<std::string, std::string>>();

      vertex.sources.emplace_back(src);
    }

    import_config.import_schema.vertices.emplace_back(vertex);
  }

  auto edges_list = schema_dict["edges"].cast<std::vector<py::dict>>();
  for (const auto& edge_dict : edges_list) {
    Edge edge;
    edge.edge_type = edge_dict["edge_type"].cast<std::string>();
    edge.src_type = edge_dict["src_type"].cast<std::string>();
    edge.src_prop = edge_dict["src_prop"].cast<std::string>();
    edge.src_edge_prop = edge_dict["src_edge_prop"].cast<std::string>();
    edge.dst_type = edge_dict["dst_type"].cast<std::string>();
    edge.dst_prop = edge_dict["dst_prop"].cast<std::string>();
    edge.dst_edge_prop = edge_dict["dst_edge_prop"].cast<std::string>();
    edge.chunk_size = edge_dict["chunk_size"].cast<int>();
    edge.validate_level = edge_dict["validate_level"].cast<std::string>();
    edge.prefix = edge_dict["prefix"].cast<std::string>();

    auto adj_list_dicts = edge_dict["adj_lists"].cast<std::vector<py::dict>>();
    for (const auto& adj_list_dict : adj_list_dicts) {
      AdjList adj_list;
      adj_list.ordered = adj_list_dict["ordered"].cast<bool>();
      adj_list.aligned_by = adj_list_dict["aligned_by"].cast<std::string>();
      adj_list.file_type = adj_list_dict["file_type"].cast<std::string>();
      edge.adj_lists.emplace_back(adj_list);
    }

    auto edge_pg_list =
        edge_dict["property_groups"].cast<std::vector<py::dict>>();
    for (const auto& edge_pg_dict : edge_pg_list) {
      PropertyGroup edge_pg;
      edge_pg.file_type = edge_pg_dict["file_type"].cast<std::string>();
      auto edge_prop_list =
          edge_pg_dict["properties"].cast<std::vector<py::dict>>();

      for (const auto& prop_dict : edge_prop_list) {
        Property edge_prop;
        edge_prop.name = prop_dict["name"].cast<std::string>();
        edge_prop.data_type = prop_dict["data_type"].cast<std::string>();
        edge_prop.is_primary = prop_dict["is_primary"].cast<bool>();
        edge_prop.nullable = prop_dict["nullable"].cast<bool>();
        edge_pg.properties.emplace_back(edge_prop);
      }

      edge.property_groups.emplace_back(edge_pg);
    }

    auto edge_source_list = edge_dict["sources"].cast<std::vector<py::dict>>();
    for (const auto& edge_source_dict : edge_source_list) {
      Source edge_src;
      edge_src.file_type = edge_source_dict["file_type"].cast<std::string>();
      edge_src.path = edge_source_dict["path"].cast<std::vector<std::string>>();
      edge_src.delimiter = edge_source_dict["delimiter"].cast<char>();
      edge_src.columns =
          edge_source_dict["columns"]
              .cast<std::unordered_map<std::string, std::string>>();

      edge.sources.emplace_back(edge_src);
    }

    import_config.import_schema.edges.emplace_back(edge);
  }

  return import_config;
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <filesystem>

#include "arrow/api.h"
#include "graphar/api/arrow_writer.h"
#include "graphar/api/high_level_writer.h"
#include "graphar/convert_to_arrow_type.h"
#include "graphar/graph_info.h"
#include "graphar/high-level/graph_reader.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include "util.h"
#include <iostream>

namespace py = pybind11;
namespace fs = std::filesystem;

struct GraphArConfig {
  std::string path;
  std::string name;
  std::string version;
};

struct Property {
  std::string name;
  std::string data_type;
  bool is_primary;
  bool nullable;
};

struct PropertyGroup {
  std::string file_type;
  std::vector<Property> properties;
};

struct Source {
  std::string file_type;
  std::vector<std::string> path;
  char delimiter;
  std::unordered_map<std::string, std::string> columns;
};

struct Vertex {
  std::string type;
  std::vector<std::string> labels;
  int chunk_size;
  std::string validate_level;
  std::string prefix;
  std::vector<PropertyGroup> property_groups;
  std::vector<Source> sources;
};

struct AdjList {
  bool ordered;
  std::string aligned_by;
  std::string file_type;
};

struct Edge {
  std::string edge_type;
  std::string src_type;
  std::string src_prop;
  std::string src_edge_prop;
  std::string dst_type;
  std::string dst_prop;
  std::string dst_edge_prop;
  int chunk_size;
  std::string validate_level;
  std::string prefix;
  std::vector<AdjList> adj_lists;
  std::vector<PropertyGroup> property_groups;
  std::vector<Source> sources;
};

struct ImportSchema {
  std::vector<Vertex> vertices;
  std::vector<Edge> edges;
};

struct ImportConfig {
  GraphArConfig graphar_config;
  ImportSchema import_schema;
  bool debug_mode;
};

ImportConfig ConvertPyDictToConfig(const py::dict& config_dict) {
  ImportConfig import_config;

  auto graphar_dict = config_dict["graphar"].cast<py::dict>();
  import_config.graphar_config.path = graphar_dict["path"].cast<std::string>();
  import_config.graphar_config.name = graphar_dict["name"].cast<std::string>();
  import_config.graphar_config.version =
      graphar_dict["version"].cast<std::string>();

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
      pg.file_type = pg_dict["file_type"].cast<std::string>();

      auto prop_list = pg_dict["properties"].cast<std::vector<py::dict>>();
      for (const auto& prop_dict : prop_list) {
        Property prop;
        prop.name = prop_dict["name"].cast<std::string>();
        prop.data_type = prop_dict["data_type"].cast<std::string>();
        prop.is_primary = prop_dict["is_primary"].cast<bool>();
        prop.nullable = prop_dict["nullable"].cast<bool>();
        pg.properties.emplace_back(prop);
      }
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

  import_config.debug_mode = config_dict["debug_mode"].cast<bool>();

  return import_config;
}

std::string DoImport(const py::dict& config_dict) {
  MemUsage mem_usage = MemUsage();
  auto import_config = ConvertPyDictToConfig(config_dict);

  auto version =
      graphar::InfoVersion::Parse(import_config.graphar_config.version).value();
  fs::path save_path = import_config.graphar_config.path;

  std::unordered_map<std::string, graphar::IdType> vertex_chunk_sizes;
  std::unordered_map<std::string, int64_t> vertex_counts;

  std::map<std::pair<std::string, std::string>,
           std::unordered_map<std::shared_ptr<arrow::Scalar>, graphar::IdType,
                              arrow::Scalar::Hash, arrow::Scalar::PtrsEqual>>
      vertex_prop_index_map;

  std::unordered_map<std::string, std::vector<std::string>>
      vertex_props_in_edges;
  std::map<std::pair<std::string, std::string>, graphar::Property>
      vertex_prop_property_map;
  for (const auto& edge : import_config.import_schema.edges) {
    vertex_props_in_edges[edge.src_type].emplace_back(edge.src_prop);
    vertex_props_in_edges[edge.dst_type].emplace_back(edge.dst_prop);
  }

  graphar::VertexInfoVector vertices_info;
  std::vector<std::string> vertices_labels;
  if (import_config.debug_mode) {
      std::cout << "Importing vertices - Start" << std::endl;
      mem_usage.print(false, true);
  }
  for (const auto& vertex : import_config.import_schema.vertices) {
    if (import_config.debug_mode) {
      std::cout << "Processing Vertex type: " << vertex.type << " - Start" << std::endl;
      mem_usage.print(false, true);
    }
    vertex_chunk_sizes[vertex.type] = vertex.chunk_size;

    if (import_config.debug_mode) {
      std::cout << "Processing Vertex Properties groups - Start " << std::endl;
      mem_usage.print(false, false);
    }

    auto pgs = std::vector<std::shared_ptr<graphar::PropertyGroup>>();
    std::string primary_key;
    for (const auto& pg : vertex.property_groups) {
      std::vector<graphar::Property> props;
      for (const auto& prop : pg.properties) {
        if (prop.is_primary) {
          if (!primary_key.empty()) {
            throw std::runtime_error("Multiple primary keys found in vertex " +
                                     vertex.type);
          }
          primary_key = prop.name;
        }
        graphar::Property property(
            prop.name, graphar::DataType::TypeNameToDataType(prop.data_type),
            prop.is_primary, prop.nullable);
        props.push_back(property);
        vertex_prop_property_map[std::make_pair(vertex.type, prop.name)] =
            property;
      }
      // TODO: add prefix parameter in config
      auto property_group = graphar::CreatePropertyGroup(
          props, graphar::StringToFileType(pg.file_type));
      pgs.emplace_back(property_group);
    }
    if (import_config.debug_mode) {
      std::cout << "Processing Vertex Properties groups - Finish " << std::endl;\
      mem_usage.print(true, true);
    }
    auto vertex_info =
        graphar::CreateVertexInfo(vertex.type, vertex.chunk_size, pgs,
                                  vertex.labels, vertex.prefix, version);
    auto file_name = vertex.type + ".vertex.yaml";
    vertex_info->Save(save_path / file_name);
    vertices_info.push_back(vertex_info);

    if (import_config.debug_mode) {
      std::cout << "Save Vertex info\n";
      mem_usage.print(false, false);
    }
    auto save_path_str = save_path.string();
    save_path_str += "/";
    if (import_config.debug_mode) {
      std::cout << "Create Vertex writer" << std::endl;
    }
    auto vertex_prop_writer = graphar::VertexPropertyWriter::Make(
                                  vertex_info, save_path_str,
                                  StringToValidateLevel(vertex.validate_level))
                                  .value();

    if (import_config.debug_mode) {
      mem_usage.print(false, true);
    }

    std::vector<std::shared_ptr<arrow::Table>> vertex_tables;
    for (const auto& source : vertex.sources) {
      std::vector<std::string> column_names;
      for (const auto& [key, value] : source.columns) {
        column_names.emplace_back(key);
      }

      if (import_config.debug_mode) {
        std::cout << "Reading Vertex files - Start" << std::endl;
        mem_usage.print(false, false);
      }
      std::shared_ptr<arrow::Table> table;
      {
        std::vector<std::shared_ptr<arrow::Table>> file_tables(source.path.size());
        for (int i = 0; i < source.path.size(); ++i) {
          file_tables[i] = GetDataFromFile(source.path[i], column_names, source.delimiter,
                            source.file_type);
        }
        if (import_config.debug_mode) {
          std::cout << "Reading Vertex files - Finish\n";
          mem_usage.print(true, true);
          std::cout << "Concatenate Tables" << std::endl;
        }
        table = ConcatenateTables(file_tables).ValueOrDie();
        if (import_config.debug_mode) {
          mem_usage.print(false, false);
        }
      }
      if (import_config.debug_mode) {
        std::cout << "Clear Vertex file tables" << std::endl;
        mem_usage.print(true, true);
        std::cout << "Parse columns to change" << std::endl;
      }
      std::unordered_map<std::string, Property> column_prop_map;
      std::unordered_map<std::string, std::string> reversed_columns_config;
      for (const auto& [key, value] : source.columns) {
        reversed_columns_config[value] = key;
      }
      for (const auto& pg : vertex.property_groups) {
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
        // TODO: whether need to check duplicate values for primary key?
        if (!prop.nullable) {
          for (const auto& chunk : arrow_column->chunks()) {
            if (chunk->null_count() > 0) {
              throw std::runtime_error("Non-nullable column '" + column +
                                      "' has null values");
            }
          }
        }
        // TODO: check this
        if (column != prop.name ||
            arrow_column->type()->id() != arrow_data_type->id()) {
          columns_to_change[column] =
              std::make_pair(prop.name, arrow_data_type);
        }
      }
      if (import_config.debug_mode) {
        mem_usage.print(false, false);
        std::cout << "Change columns Vertex" << std::endl;
      }
      table = ChangeNameAndDataType(table, columns_to_change);
      if (import_config.debug_mode) {
        mem_usage.print(true, true);
        std::cout << "Add Vertex table" << std::endl;
      }
      vertex_tables.push_back(table);
      if (import_config.debug_mode) {
        mem_usage.print(false, true);
        std::cout << "Finish source" << std::endl;
      }
    }
    if (import_config.debug_mode) {
      mem_usage.print(true, false);
      std::cout << "Merge vertex table" << std::endl;
    }
    std::shared_ptr<arrow::Table> merged_vertex_table =
        MergeTables(vertex_tables);
    // TODO: check all fields in props
    // TODO: add start_index in config
    graphar::IdType start_chunk_index = 0;

    if (import_config.debug_mode) {
      mem_usage.print(false, false);
      std::cout << "Add Index Column" << std::endl;
    }
    auto vertex_table_with_index =
        vertex_prop_writer
            ->AddIndexColumn(merged_vertex_table, start_chunk_index,
                             vertex_info->GetChunkSize())
            .value();
    if (import_config.debug_mode) {
      mem_usage.print(true, true);
      std::cout << "Writing Vertex - Start" << std::endl;
    }
    for (const auto& property_group : pgs) {
      if (import_config.debug_mode) {
        std::cout << "Property group: " << property_group->GetPrefix() << '\n';
      }
      vertex_prop_writer->WriteTable(vertex_table_with_index, property_group,
                                     start_chunk_index);
      if (import_config.debug_mode) {
        mem_usage.print(false, true);
      }
    }
    auto vertex_count = merged_vertex_table->num_rows();
    vertex_counts[vertex.type] = vertex_count;
    vertex_prop_writer->WriteVerticesNum(vertex_count);
    if (import_config.debug_mode) {
      std::cout << "Save Vertex count: " << vertex_count << std::endl;
      mem_usage.print(false, true);
    }
    if (import_config.debug_mode) {
      std::cout << "Writing Vertex - Finish" << std::endl;
      mem_usage.print(false, true);
    }
    for (auto &label : vertex.labels) {
      vertices_labels.push_back(label);
    }
    if (vertex_props_in_edges.find(vertex.type) !=
        vertex_props_in_edges.end()) {
      if (import_config.debug_mode) {
        std::cout << "Edges2Vertex property map - Start" << std::endl;
      }
      for (const auto& vertex_prop : vertex_props_in_edges[vertex.type]) {
        if (import_config.debug_mode) {
          std::cout << "Map property: " << vertex_prop << std::endl;
        }
        if (vertex_prop_index_map.find(std::make_pair(vertex.type, vertex_prop)) == vertex_prop_index_map.end()) {
          vertex_prop_index_map[std::make_pair(vertex.type, vertex_prop)] =
              TableToUnorderedMap(vertex_table_with_index, vertex_prop,
                                  graphar::GeneralParams::kVertexIndexCol);
        } else if (import_config.debug_mode) {
          std::cout << "- Duplicate property SKIP\n";
        }
        if (import_config.debug_mode) {
          mem_usage.print(false, true);
        }
      }
      if (import_config.debug_mode) {
        std::cout << "Edges2Vertex property map - Finish\n";
        mem_usage.print(false, true);
      }
    }
    if (import_config.debug_mode) {
      std::cout << "Processing Vertex type: " << vertex.type << " - Finish\n";
      mem_usage.print(false, true);
    }
  }
  if (import_config.debug_mode) {
    std::cout << "Importing vertices - Finish\n";
    mem_usage.print(false, false);
    std::cout << "Importing edges - Start" << std::endl;
  }
  graphar::EdgeInfoVector edges_info;
  for (const auto& edge : import_config.import_schema.edges) {
    if (import_config.debug_mode) {
      std::cout << "Processing Edge type: " << edge.edge_type << " - Start\n";
      mem_usage.print(false, true);
    }
    auto pgs = std::vector<std::shared_ptr<graphar::PropertyGroup>>();

    if (import_config.debug_mode) {
      std::cout << "Parse Edge Properties groups - Start " << std::endl;
      mem_usage.print(false, true);
    }
    for (const auto& pg : edge.property_groups) {
      std::vector<graphar::Property> props;
      for (const auto& prop : pg.properties) {
        props.push_back(graphar::Property(
                        prop.name, graphar::DataType::TypeNameToDataType(prop.data_type),
                        prop.is_primary, prop.nullable));
      }
      // TODO: add prefix parameter in config
      auto property_group = graphar::CreatePropertyGroup(
          props, graphar::StringToFileType(pg.file_type));
      pgs.push_back(property_group);
    }
    if (import_config.debug_mode) {
      std::cout << "Parse Edge Properties groups - Finish\n";
      mem_usage.print(true, true);
      std::cout << "Parse Edge Adjacent List - Start" << std::endl;
    }
    graphar::AdjacentListVector adj_lists;
    for (const auto& adj_list : edge.adj_lists) {
      // TODO: add prefix parameter in config
      adj_lists.push_back(graphar::CreateAdjacentList(
          graphar::OrderedAlignedToAdjListType(adj_list.ordered,
                                               adj_list.aligned_by),
          graphar::StringToFileType(adj_list.file_type)));
    }
    if (import_config.debug_mode) {
      std::cout << "Parse Edge Adjacent List - Finish " << std::endl;
      mem_usage.print(false, true);
    }

    // TODO: add directed parameter in config

    bool directed = true;
    // TODO: whether prefix has default value?

    auto edge_info = graphar::CreateEdgeInfo(
        edge.src_type, edge.edge_type, edge.dst_type, edge.chunk_size,
        vertex_chunk_sizes[edge.src_type], vertex_chunk_sizes[edge.dst_type],
        directed, adj_lists, pgs, edge.prefix, version);
    auto file_name =
        ConcatEdgeTriple(edge.src_type, edge.edge_type, edge.dst_type) +
        ".edge.yaml";
    edge_info->Save(save_path / file_name);
    edges_info.push_back(edge_info);
    if (import_config.debug_mode) {
      std::cout << "Saved Edge info" << std::endl;
      mem_usage.print(false, true);
    }
    auto save_path_str = save_path.string();
    save_path_str += "/";

    std::vector<int64_t> from_sizes((vertex_counts[edge.src_type] - 1) / vertex_chunk_sizes[edge.src_type] + 1);
    int64_t from_chunk_size = vertex_chunk_sizes[edge.src_type];
    std::vector<int64_t> to_sizes((vertex_counts[edge.dst_type] - 1) / vertex_chunk_sizes[edge.dst_type] + 1);
    int64_t to_chunk_size = vertex_chunk_sizes[edge.dst_type];
    bool first_adj = true;
    for (const auto& adj_list : adj_lists) {
      if (import_config.debug_mode) {
        std::cout << "Processing Adjacent List: ";

        if (adj_list->GetType() == graphar::AdjListType::ordered_by_source) {
          std::cout << "ordered_by_source";
        } else if (adj_list->GetType() == graphar::AdjListType::ordered_by_dest) {
          std::cout << "ordered_by_dest";
        }
        std::cout << "- Start" << std::endl;
        mem_usage.print(false, true);
      }

      int64_t vertex_count;
      if (adj_list->GetType() == graphar::AdjListType::ordered_by_source ||
          adj_list->GetType() == graphar::AdjListType::unordered_by_source) {
        vertex_count = vertex_counts[edge.src_type];
      } else {
        vertex_count = vertex_counts[edge.dst_type];
      }
      std::vector<std::shared_ptr<arrow::Table>> edge_tables;

      for (const auto& source : edge.sources) {
        std::vector<std::string> column_names;
        for (const auto& [key, value] : source.columns) {
          column_names.emplace_back(key);
        }
        if (import_config.debug_mode) {
          std::cout << "Reading Edge files - Start" << std::endl;
          mem_usage.print(false, true);
        }

        std::shared_ptr<arrow::Table> table;
        {
          std::vector<std::shared_ptr<arrow::Table>> file_tables(source.path.size());
          for (int i = 0; i < source.path.size(); ++i) {
            file_tables[i] = GetDataFromFile(source.path[i], column_names,
                                             source.delimiter, source.file_type);
          }
          if (import_config.debug_mode) {
            std::cout << "Reading Edge files - Finish\n";
            mem_usage.print(true, true);
            std::cout << "Concatenate Tables" << std::endl;
          }
          table = ConcatenateTables(file_tables).ValueOrDie();
          if (import_config.debug_mode) {
            mem_usage.print(false, false);
          }
        }
        if (import_config.debug_mode) {
          std::cout << "Clear Edge file tables" << std::endl;
          mem_usage.print(false, true);
        }
        std::unordered_map<std::string, graphar::Property> column_prop_map;
        std::unordered_map<std::string, std::string> reversed_columns;
        for (const auto& [key, value] : source.columns) {
          reversed_columns[value] = key;
        }

        for (const auto& pg : edge.property_groups) {
          for (const auto& prop : pg.properties) {
            column_prop_map[reversed_columns[prop.name]] = graphar::Property(
                prop.name,
                graphar::DataType::TypeNameToDataType(prop.data_type),
                prop.is_primary, prop.nullable);
          }
        }
        if (import_config.debug_mode) {
          std::cout << "Properties map created" << std::endl;
          mem_usage.print(false, false);
        }
        const auto &src_prop = vertex_prop_property_map.at(std::make_pair(edge.src_type, edge.src_prop));
        column_prop_map[reversed_columns.at(edge.src_edge_prop)] = graphar::Property(
            edge.src_edge_prop,
            src_prop.type,
            src_prop.is_primary, src_prop.is_nullable);
        const auto &dst_prop = vertex_prop_property_map.at(std::make_pair(edge.dst_type, edge.dst_prop));
        column_prop_map[reversed_columns.at(edge.dst_edge_prop)] = graphar::Property(
            edge.dst_edge_prop,
            dst_prop.type,
            dst_prop.is_primary, dst_prop.is_nullable);
        if (import_config.debug_mode) {
          std::cout << "Added src and dst edge properties" << std::endl;
          mem_usage.print(true, true);
          std::cout << "Processing columns_to_change" << std::endl;
        }
        std::unordered_map<
            std::string,
            std::pair<std::string, std::shared_ptr<arrow::DataType>>>
            columns_to_change;
        for (const auto& [column, prop] : column_prop_map) {
          auto arrow_data_type =
              graphar::DataType::DataTypeToArrowDataType(prop.type);
          auto arrow_column = table->GetColumnByName(column);
          // TODO: is needed?
          if (!prop.is_nullable) {
            for (const auto& chunk : arrow_column->chunks()) {
              if (chunk->null_count() > 0) {
                throw std::runtime_error("Non-nullable column '" + column +
                                         "' has null values");
              }
            }
          }
          if (column != prop.name ||
              arrow_column->type()->id() != arrow_data_type->id()) {
            columns_to_change[column] =
                std::make_pair(prop.name, arrow_data_type);
          }
        }
        if (import_config.debug_mode) {
          mem_usage.print(false, false);
          std::cout << "Change columns edge" << std::endl;
        }
        table = ChangeNameAndDataType(table, columns_to_change);
        edge_tables.push_back(table);
        if (import_config.debug_mode) {
          mem_usage.print(false, true);
        }
      }
      std::unordered_map<
          std::string, std::pair<std::string, std::shared_ptr<arrow::DataType>>>
          vertex_columns_to_change;
      if (import_config.debug_mode) {
        mem_usage.print(true, false);
        std::cout << "Merge edge table" << std::endl;
      }
      std::shared_ptr<arrow::Table> merged_edge_table =
          MergeTables(edge_tables);
      // TODO: check all fields in props
      if (import_config.debug_mode) {
        mem_usage.print(true, false);
        std::cout << "Merge CombineChunks" << std::endl;
      }
      auto combined_edge_table =
          merged_edge_table->CombineChunks().ValueOrDie();
      if (import_config.debug_mode) {
        mem_usage.print(false, true);
      }

      auto edge_builder =
          graphar::builder::EdgesBuilder::Make(
              edge_info, save_path_str, adj_list->GetType(), vertex_count,
              StringToValidateLevel(edge.validate_level))
              .value();

      if (import_config.debug_mode) {
        std::cout << "Edge columns:";
      }
      std::vector<std::string> edge_column_names;
      for (const auto& field : combined_edge_table->schema()->fields()) {
        edge_column_names.push_back(field->name());
        if (import_config.debug_mode) {
          std::cout << ' ' << field->name();
        }
      }
      const int64_t num_rows = combined_edge_table->num_rows();

      if (import_config.debug_mode) {
        std::cout << "\nAdding edges - Start\n";
        mem_usage.print(false, true);
        std::cout << "Prop:";
        for (const auto& column_name : edge_column_names) {
          if (column_name != edge.src_edge_prop && column_name != edge.dst_edge_prop) {
            std::cout << ' ' << column_name;
          }
        }
        std::cout << '\n';
      }
      if (first_adj) {
        first_adj = false;
        for (int64_t i = 0; i < num_rows; ++i) {
          auto edge_src_column =
                  combined_edge_table->GetColumnByName(edge.src_edge_prop);
          auto edge_dst_column =
                  combined_edge_table->GetColumnByName(edge.dst_edge_prop);

          from_sizes[vertex_prop_index_map
                             .at(std::make_pair(edge.src_type, edge.src_prop))
                             .at(edge_src_column->GetScalar(i).ValueOrDie())
                     / from_chunk_size]++;
          to_sizes[vertex_prop_index_map
                           .at(std::make_pair(edge.dst_type, edge.dst_prop))
                           .at(edge_dst_column->GetScalar(i).ValueOrDie()) / to_chunk_size]++;
          if (import_config.debug_mode && (i % 100000000 == 0)) {
            std::cout << "PreCalc " << i + 1 << "/" << num_rows << std::endl;
          }
        }
        if (import_config.debug_mode) {
          std::cout << "Preprocess: " << from_sizes.size() << " chunks\n";
          mem_usage.print(false, true);
        }
      }

      if (adj_list->GetType() == graphar::AdjListType::ordered_by_source ||
          adj_list->GetType() == graphar::AdjListType::unordered_by_source) {
        edge_builder->PreReserve(from_sizes);
      } else {
        edge_builder->PreReserve(to_sizes);
      }

      if (import_config.debug_mode) {
        std::cout << "Reserved edges\n";
        mem_usage.print(false, true);
      }

      for (int64_t i = 0; i < num_rows; ++i) {
        auto edge_src_column =
            combined_edge_table->GetColumnByName(edge.src_edge_prop);
        auto edge_dst_column =
            combined_edge_table->GetColumnByName(edge.dst_edge_prop);

        graphar::builder::Edge e(
            vertex_prop_index_map
                .at(std::make_pair(edge.src_type, edge.src_prop))
                .at(edge_src_column->GetScalar(i).ValueOrDie()),
            vertex_prop_index_map
                .at(std::make_pair(edge.dst_type, edge.dst_prop))
                .at(edge_dst_column->GetScalar(i).ValueOrDie()));
        for (const auto& column_name : edge_column_names) {
          if (column_name != edge.src_edge_prop && column_name != edge.dst_edge_prop) {
            auto column = combined_edge_table->GetColumnByName(column_name);
            auto column_type = column->type();
            std::any value;
            TryToCastToAny(
                graphar::DataType::ArrowDataTypeToDataType(column_type),
                column->chunk(0), value, i);
            if (value.has_value()) {
              e.AddProperty(column_name, value);
            }
          }
        }
        if (import_config.debug_mode) {
          if (i % 10000000 == 0) {
            mem_usage.print(true, false);
            std::cout << "Add " << i + 1 << "/" << num_rows << "\n";
          }
        }
        edge_builder->AddEdge(e);
        if (import_config.debug_mode && (i % 10000000 == 0)) {
          mem_usage.print(false, true);
        }
      }
      if (import_config.debug_mode) {
        std::cout << "Adding edges - Finish" << std::endl;
        mem_usage.print(false, true);
        std::cout << "Dump edge" << edge.edge_type << std::endl;
        mem_usage.print();
      }

      edge_builder->Dump();

      if (import_config.debug_mode) {
        std::cout << "Processing Adjacent List - Finish\n";
        mem_usage.print(false, true);
      }
    }
    if (import_config.debug_mode) {
      std::cout << "Processing Edge type: " << edge.edge_type << " - Finish" << std::endl;
      mem_usage.print(false, true);
    }
  }
  if (import_config.debug_mode) {
    std::cout << "Importing edges - Finish\n";
    mem_usage.print(false, true);
  }
  auto graph_info = graphar::CreateGraphInfo(import_config.graphar_config.name,
                                              vertices_info, edges_info, vertices_labels, "./", version);
  auto file_name = graph_info->GetName() + ".yaml";
  graph_info->Save(save_path / file_name);

  return "Imported successfully!";
}
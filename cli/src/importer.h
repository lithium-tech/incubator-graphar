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
#include "tools.h"

#include "util.h"
#include <iostream>
#include <string_view>
#include <omp.h>

#include <unistd.h>
#include <fstream>
#include <malloc.h>

std::string DoImport(const py::dict& config_dict) {
  logger("Start of import");

  ImportConfig import_config;
  import_config.fill(config_dict);

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
    vertex_props_in_edges[edge.src_type].emplace_back(edge.src_prop);  //duplicates, but they weight not so much
    vertex_props_in_edges[edge.dst_type].emplace_back(edge.dst_prop);
  }

  graphar::VertexInfoVector vertices_info;
  std::vector<std::string> vertices_labels;
  logger("Processing vertices");
  for (const auto& vertex : import_config.import_schema.vertices) {
    vertex_chunk_sizes[vertex.type] = vertex.chunk_size;

    auto pgs = std::vector<std::shared_ptr<graphar::PropertyGroup>>();
    std::string primary_key;
    int number_of_pgroups = 0;
    for (const auto& pg : vertex.property_groups) {
      std::vector<graphar::Property> props;
      ++number_of_pgroups;
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
      // TODO: add prefix parameter in config !!!
      auto property_group = graphar::CreatePropertyGroup(
          props, graphar::StringToFileType(pg.file_type), 
          vertex.type+"_properties_"+std::to_string(number_of_pgroups));
      pgs.emplace_back(property_group);
    }
    auto vertex_info =
        graphar::CreateVertexInfo(vertex.type, vertex.chunk_size, pgs,
                                  vertex.labels, vertex.prefix, version);
    auto file_name = vertex.type + ".vertex.yaml";
    vertex_info->Save(save_path / file_name);
    vertices_info.push_back(vertex_info);
    logger("Vertex info saved");

    auto save_path_str = save_path.string();
    save_path_str += "/";
    auto vertex_prop_writer = graphar::VertexPropertyWriter::Make(
                                  vertex_info, save_path_str,
                                  StringToValidateLevel(vertex.validate_level))
                                  .value();

    std::vector<std::shared_ptr<arrow::Table>> vertex_tables;
    for (const auto& source : vertex.sources) {
      std::vector<std::string> column_names;
      for (const auto& [key, value] : source.columns) {
        column_names.emplace_back(key);
      }
      logger("  Source columns collected: "+std::to_string(column_names.size()));

      std::shared_ptr<arrow::Table> table;
      {
        std::vector<std::shared_ptr<arrow::Table>> file_tables(source.path.size());
        for (int i = 0; i < source.path.size(); ++i) {
          file_tables[i] = GetDataFromFile(source.path[i], column_names, source.delimiter,
                            source.file_type);
        }
        table = ConcatenateTables(file_tables).ValueOrDie();
      }
      logger("Vertex sources read.");

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
      std::cout << "Size of column_prop_map: " << column_prop_map.size() << std::endl;
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
      table = ChangeNameAndDataType(table, columns_to_change);
      vertex_tables.push_back(table);
    }
    std::shared_ptr<arrow::Table> merged_vertex_table =
        MergeTables(vertex_tables);
    // TODO: check all fields in props
    // TODO: add start_index in config
    graphar::IdType start_chunk_index = 0;

    auto vertex_table_with_index =
        vertex_prop_writer
            ->AddIndexColumn(merged_vertex_table, start_chunk_index,
                             vertex_info->GetChunkSize())
            .value();
    logger("Vertex table with index created");

    for (const auto& property_group : pgs) {
      vertex_prop_writer->WriteTable(vertex_table_with_index, property_group,
                                     start_chunk_index);
    }
    logger("Wrote "+std::to_string(pgs.size())+" property tables.");
    auto vertex_count = merged_vertex_table->num_rows();
    vertex_counts[vertex.type] = vertex_count;
    vertex_prop_writer->WriteVerticesNum(vertex_count);
    for (auto &label : vertex.labels) {
      vertices_labels.push_back(label);
    }
    if (vertex_props_in_edges.find(vertex.type) !=
        vertex_props_in_edges.end()) {
      for (const auto& vertex_prop : vertex_props_in_edges[vertex.type]) {
        if (vertex_prop_index_map.find(std::make_pair(vertex.type, vertex_prop)) == vertex_prop_index_map.end()) {
          vertex_prop_index_map[std::make_pair(vertex.type, vertex_prop)] =
              TableToUnorderedMap(vertex_table_with_index, vertex_prop,
                                  graphar::GeneralParams::kVertexIndexCol);
        }
      }
    }
  }

  logger("Procissing edges");
  graphar::EdgeInfoVector edges_info;
  for (const auto& edge : import_config.import_schema.edges) {
    logger("Processing edge <"+edge.edge_type+">: start");

    auto pgs = std::vector<std::shared_ptr<graphar::PropertyGroup>>();
    
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
    logger("  edge <"+edge.edge_type+">Property groups: end ("+std::to_string(edge.property_groups.size()+1)+" processed)");
    graphar::AdjacentListVector adj_lists;
    for (const auto& adj_list : edge.adj_lists) {
      // TODO: add prefix parameter in config
      adj_lists.push_back(graphar::CreateAdjacentList(
          graphar::OrderedAlignedToAdjListType(adj_list.ordered,
                                               adj_list.aligned_by),
          graphar::StringToFileType(adj_list.file_type)));
    }
    logger("  edge <"+edge.edge_type+"> Adj list: end");
    // TODO: add directed parameter in config

    bool directed = true;
    // TODO: whether prefix has default value?

    auto edge_info = graphar::CreateEdgeInfo(
        edge.src_type, edge.edge_type, edge.dst_type, edge.chunk_size,
        vertex_chunk_sizes[edge.src_type], vertex_chunk_sizes[edge.dst_type],
        directed, adj_lists, pgs, edge.prefix, version);  //[MY TODO] pgs can be deleted now
    auto file_name =
        ConcatEdgeTriple(edge.src_type, edge.edge_type, edge.dst_type) +
        ".edge.yaml";
    edge_info->Save(save_path / file_name);
    edges_info.push_back(edge_info);
    auto save_path_str = save_path.string();
    save_path_str += "/";

    logger("edge <"+edge.edge_type+">.yaml saved");

    int64_t from_chunk_size = vertex_chunk_sizes[edge.src_type];
    int64_t to_chunk_size = vertex_chunk_sizes[edge.dst_type];
    
    logger("edge <"+edge.edge_type+"> adjacent list: start");
    bool first_adj = true;
    for (const auto& adj_list : adj_lists) {

      int64_t vertex_count;
      if (adj_list->GetType() == graphar::AdjListType::ordered_by_source ||
          adj_list->GetType() == graphar::AdjListType::unordered_by_source) {
        vertex_count = vertex_counts[edge.src_type];
      } else {
        vertex_count = vertex_counts[edge.dst_type];
      }
      std::vector<std::shared_ptr<arrow::Table>> edge_tables;

      for (const auto& source : edge.sources) {
        logger("edge <"+edge.edge_type+"> reading files: start");

        std::vector<std::string> column_names; //[MY TODO] resrve
        for (const auto& [key, value] : source.columns) {
          column_names.emplace_back(key);
        }

        std::shared_ptr<arrow::Table> table;
        {
          std::vector<std::shared_ptr<arrow::Table>> file_tables(source.path.size());
          for (int i = 0; i < source.path.size(); ++i) {
            file_tables[i] = GetDataFromFile(source.path[i], column_names,
                                             source.delimiter, source.file_type);
          }
          table = ConcatenateTables(file_tables).ValueOrDie();
          logger("edge <"+edge.edge_type+"> "+std::to_string(source.path.size()) +" tables concatenated");
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
        logger("edge <"+edge.edge_type+"> property groups: end");

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
        logger("Name and data type are about to change.");
        table = ChangeNameAndDataType(table, columns_to_change);
        logger("edge <"+edge.edge_type+"> table columns changed");
        edge_tables.push_back(table);
        logger("edge <"+edge.edge_type+"> table added");
      }
      std::unordered_map<
          std::string, std::pair<std::string, std::shared_ptr<arrow::DataType>>>
          vertex_columns_to_change;  
      std::shared_ptr<arrow::Table> merged_edge_table =
          MergeTables(edge_tables);
      logger("Tables merged");
      // TODO: check all fields in props
      auto combined_edge_table =
          merged_edge_table->CombineChunks().ValueOrDie();  //[My] we moved edge_tables, so memory should not raise
      logger("Combined_edge_table created");
      auto edge_builder =
          graphar::builder::EdgesBuilder::Make(
              edge_info, save_path_str, adj_list->GetType(), vertex_count,
              StringToValidateLevel(edge.validate_level))
              .value();
      logger("Edge_builder created"); 

      std::vector<std::string> edge_column_names;
      for (const auto& field : combined_edge_table->schema()->fields()) {
        edge_column_names.push_back(field->name());
      }
      logger("edge_column_names created");

      const int64_t num_rows = combined_edge_table->num_rows();

      //(multi-thread) mapping row to its' bucket
      int num_threads =  omp_get_max_threads() / 2;
      std::cout << "Multi-tread mapping: starts in " << num_threads << " treads" << std::endl;
      
      //calculating num of chunks
      int num_of_chunks = 0;
      if(adj_list->GetType() == graphar::AdjListType::ordered_by_source ||
        adj_list->GetType() == graphar::AdjListType::unordered_by_source)
      {
        num_of_chunks = (vertex_counts[edge.src_type] - 1) / vertex_chunk_sizes[edge.src_type] + 1;
      }
      else
      {
        num_of_chunks = (vertex_counts[edge.dst_type] - 1) / vertex_chunk_sizes[edge.dst_type] + 1;
      }

      //creating vector that maps row to its' chunk: [num_of_threads][num_of_chunks][rows]
      std::vector<std::vector<std::vector<int64_t>>> edge_to_chunk_mapping(
        num_threads,
        std::vector<std::vector<int64_t>>(num_of_chunks)
      );

      auto edge_src = combined_edge_table->GetColumnByName(edge.src_edge_prop)->GetScalar(0).ValueOrDie();
      auto edge_dst = combined_edge_table->GetColumnByName(edge.dst_edge_prop)->GetScalar(0).ValueOrDie();

      //mapping each row to its' chunk
      std::cout << "Num rows: " << num_rows << std::endl;
      #pragma omp parallel for schedule(static) num_threads(num_threads)
      for (int64_t row = 0; row < num_rows; ++row)
      {
        int64_t chunk = 0;
        bool failed = false; //debug
        if (adj_list->GetType() == graphar::AdjListType::ordered_by_source ||
            adj_list->GetType() == graphar::AdjListType::unordered_by_source)
        {
          auto edge_src_column =  // getting column
                combined_edge_table->GetColumnByName(edge.src_edge_prop);

          auto edge_src = edge_src_column->GetScalar(row).ValueOrDie();
          auto src_key = std::make_pair(edge.src_type, edge.src_prop);
          auto& src_map = vertex_prop_index_map.at(src_key);
          auto val = src_map.find(edge_src);

          if (val == src_map.end())
          {
            #pragma omp critical
            {
              std::cout << "!!! Could not find object in vertex_prop_index_map, row (src):" << row 
                        << "type: " << edge_src->type->ToString() 
                        << "thead: " << omp_get_thread_num()
                        << std::endl;
            }
            failed = true;
          }
          if(!failed)
          {
            chunk = val->second / edge_info->GetSrcChunkSize();
          }
        }
        else
        {
          auto edge_dst_column =
                combined_edge_table->GetColumnByName(edge.dst_edge_prop);
          auto edge_dst = edge_dst_column->GetScalar(row).ValueOrDie();
          auto dst_key = std::make_pair(edge.dst_type, edge.dst_prop);
          auto& dst_map = vertex_prop_index_map.at(dst_key);
          auto val = dst_map.find(edge_dst);

          if (val == dst_map.end())
          {
            #pragma omp critical
            {
              std::cout << "!!! Could not find object in vertex_prop_index_map, row (dst):" << row 
                        << "type: " << edge_dst->type->ToString() 
                        << "thead: " << omp_get_thread_num()
                        << std::endl;
            }
            failed = true;
          }
          if (!failed)
          {
            chunk = val->second / edge_info->GetDstChunkSize();
          }
        }
        if(failed)
        {
          #pragma omp critical
          {
            for (int col = 0; col < combined_edge_table->num_columns(); ++col) {
              const auto& field = combined_edge_table->schema()->field(col);
              std::string column_name = field->name();

              auto column = combined_edge_table->column(col);
              auto scalar = column->GetScalar(row).ValueOrDie();
              std::string value_str = scalar->ToString();
              std::cout << "col: " << column_name << " value: " << value_str << std::endl;
            }
          }
        }
        else
        {
          edge_to_chunk_mapping[omp_get_thread_num()][chunk].push_back(row);
          /*if(row % 1000000000 == 0)
          {
            malloc_stats();
          }*/
        }
      }
      
      // add column names
      for (const auto& column_name : edge_column_names) {
        if (column_name != edge.src_edge_prop && column_name != edge.dst_edge_prop) {
          edge_builder->AddColumnName(column_name);
        }
      }

      //starting edge building: adding edges chunk-wise
      logger("Edge building: start");
      int processed_chunks = 0;
      #pragma omp parallel for schedule(dynamic) num_threads(num_threads)
      for(int chunk = 0; chunk < num_of_chunks; ++chunk)
      {
        auto edge_src_column =
          combined_edge_table->GetColumnByName(edge.src_edge_prop);
        auto edge_dst_column =
          combined_edge_table->GetColumnByName(edge.dst_edge_prop);

        //we should gather each thread's info about rows in this very chuck 
        for(int thread_output = 0; thread_output < num_threads; ++thread_output)
        {
          //visit all the rows this thread found for this chunk
          for(auto i : edge_to_chunk_mapping[thread_output][chunk])
          {
            //create an edge
            bool failed = false;

            //debug: check src
            auto src_key = std::make_pair(edge.src_type, edge.src_prop);
            auto edge_src = edge_src_column->GetScalar(i).ValueOrDie();
            auto& src_map = vertex_prop_index_map[src_key];
            auto val_src = src_map.find(edge_src);
            if (val_src == src_map.end())
            {
              std::cout << "!!! Could not find object in vertex_prop_index_map, row (src):" << i 
                        << "type: " << edge_src->type->ToString() 
                        << "thead: " << omp_get_thread_num()
                        << std::endl;
              failed = true;
            }

            //debug: check dst
            auto edge_dst = edge_dst_column->GetScalar(i).ValueOrDie();
            auto dst_key = std::make_pair(edge.dst_type, edge.dst_prop);
            auto& dst_map = vertex_prop_index_map[dst_key];
            auto val_dst = dst_map.find(edge_dst);
            if (val_dst == dst_map.end())
            {
              std::cout << "!!! Could not find object in vertex_prop_index_map, row (dst):" << i 
                        << "type: " << edge_dst->type->ToString() 
                        << "thead: " << omp_get_thread_num()
                        << std::endl;
              failed = true;
            }

            if (failed)
            {
              continue;
            }

            graphar::builder::Edge e(
              val_src->second,   //src id
              val_dst->second    //dst id
            );

            //reserve space for its' properties
            e.Reserve(edge_column_names.size()-2);

            //find properties and add them
            for (const auto& column_name : edge_column_names) {
              if (column_name != edge.src_edge_prop && column_name != edge.dst_edge_prop) {
                auto column = combined_edge_table->GetColumnByName(column_name);
                auto column_type = column->type();
                std::any value;
                TryToCastToAny(
                    graphar::DataType::ArrowDataTypeToDataType(column_type),
                    column->chunk(0), value, i);
                if (value.has_value()) {
                  e.AddProperty(edge_builder->GetColumnName(column_name), value);  
                }
              }
            }
            edge_builder->AddEdge(e);
          }
        }
        //the chunk is ready, dump it
        edge_builder->Dump(chunk);

        #pragma omp atomic
        ++processed_chunks;

        logger("chunk: "+std::to_string(processed_chunks)+"/"+std::to_string(num_of_chunks));
      }
    }
  }

  logger("CreateGraphInfo: start");
  auto graph_info = graphar::CreateGraphInfo(import_config.graphar_config.name,
                                              vertices_info, edges_info, vertices_labels, "./", version);
  auto file_name = graph_info->GetName() + ".yaml";
  graph_info->Save(save_path / file_name);
  logger("Save: end");

  return "Imported successfully!";
}
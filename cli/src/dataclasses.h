/*
* This file contains data structures used to parse configs 
* and functions that do parsing.
*/

#pragma once

#include <string>
#include <vector>
#include <unordered_map>

namespace py = pybind11;

struct GraphArConfig {
  std::string path;
  std::string name;
  std::string version;

  void fill(const py::dict& config_dict);
};

struct Property {
  std::string name;
  std::string data_type;
  bool is_primary;
  bool nullable;

  void fill(const py::dict& config_dict);
};

struct PropertyGroup {
  std::string file_type;
  std::vector<Property> properties;

  void fill(const py::dict& config_dict);
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

struct MergeVertex: public Vertex {
    std::string join_on;
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

struct MergeSchema {
    std::vector<MergeVertex> vertices;
    std::vector<Edge> edges;
};

struct MergeConfig {
  GraphArConfig graphar_config;
  MergeSchema merge_schema;
  bool debug_mode;
};

struct ImportConfig {
  GraphArConfig graphar_config;
  ImportSchema import_schema;
  bool debug_mode;
};
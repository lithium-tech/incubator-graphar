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

#include <iostream>
#if defined(__linux__)
#include <fstream>
 #include <unistd.h>
#elif defined(__APPLE__)
#include <mach/mach.h>
#endif
#include <any>
#include <vector>
#include <string>
#include <cstring>
#include <sys/file.h>

#include "graphar/writer_util.h"
#include "graphar/types.h"

std::string ConcatEdgeTriple(const std::string& src_type,
                             const std::string& edge_type,
                             const std::string& dst_type);

graphar::ValidateLevel StringToValidateLevel(const std::string& level);

// Utility function to filter the columns from a table
std::shared_ptr<arrow::Table> SelectColumns(
    const std::shared_ptr<arrow::Table>& table,
    const std::vector<std::string>& column_names);

std::shared_ptr<arrow::Table> GetDataFromParquetFile(
    const std::string& path, const std::vector<std::string>& column_names);

std::shared_ptr<arrow::Table> GetDataFromCsvFile(
    const std::string& path, const std::vector<std::string>& column_names,
    const char delimiter);

#ifdef ARROW_ORC
std::shared_ptr<arrow::Table> GetDataFromOrcFile(
    const std::string& path, const std::vector<std::string>& column_names);
#endif

std::shared_ptr<arrow::Table> GetDataFromJsonFile(
    const std::string& path, const std::vector<std::string>& column_names);

std::shared_ptr<arrow::Table> GetDataFromFile(
    const std::string& path, const std::vector<std::string>& column_names,
    const char& delimiter, const std::string& file_type);


/*==================== Bin files in-out functions & settings ====================*/

struct BinHeader {
    uint32_t magic = 0x42494E31;
    uint64_t count = 0;
    uint32_t element_size = sizeof(int64_t);

    BinHeader()
        : magic(0x42494E31),
          count(0),
          element_size(sizeof(int64_t)) {}
};

void clear_directory(const std::string& path);


void clear_file(const std::string& path);

class FileDescriptor {
    int fd_;
public:
    explicit FileDescriptor(int fd) : fd_(fd) {}
    ~FileDescriptor() {
        if (fd_ != -1) close(fd_);
    }

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    FileDescriptor(FileDescriptor&& other) noexcept : fd_(other.fd_) {
        other.fd_ = -1;
    }

    int get() const { return fd_; }

    void lock_exclusive() {
        if (flock(fd_, LOCK_EX) != 0) {
            throw std::runtime_error("Failed to lock file");
        }
    }

    void unlock() {
        flock(fd_, LOCK_UN);
    }
};

FileDescriptor open_bin(const std::string& path);

ssize_t write_exact(int fd, const void* buf, size_t size, size_t offset);

void read_exact(int fd, void* buf, size_t size, off_t offset);

void write_header_bin(int fd, const BinHeader& h);

BinHeader read_header_bin(int fd);

void append_to_bin(const std::vector<int64_t>& data, const std::string& path);

int64_t get_count_bin(const std::string& path);

class FileStream {
public:
    explicit FileStream(const std::string& path);

    bool next(int64_t& value);

    uint64_t count() const {return header_.count;}

    const std::string& get_path() {return path_;}

private:
    void refill();

    FileDescriptor fd_;
    BinHeader header_;
    std::string path_;

    static constexpr size_t BUF_SIZE = 4096;
    int64_t buffer_[BUF_SIZE];

    size_t pos_ = 0;
    size_t size_ = 0;
    uint64_t read_elements_ = 0;
    off_t offset_ = 0;
};



/*==================== Table editing  ====================*/
std::shared_ptr<arrow::Table> ChangeNameAndDataType(
    const std::shared_ptr<arrow::Table>& table,
    const std::unordered_map<
        std::string, std::pair<std::string, std::shared_ptr<arrow::DataType>>>&
        columns_to_change);

std::shared_ptr<arrow::Table> MergeTables(
    const std::vector<std::shared_ptr<arrow::Table>>& tables);

template <typename KeyArrayType>
void FillMap(const std::shared_ptr<KeyArrayType>& keys,
             const std::shared_ptr<arrow::Int64Array>& values,
             std::unordered_map<int64_t, graphar::IdType>& result);

std::unordered_map<int64_t, graphar::IdType>
TableToUnorderedMapInt64(const std::shared_ptr<arrow::Table>& table,
                    const std::string& key_column_name,
                    const std::string& value_column_name);

std::unordered_map<std::shared_ptr<arrow::Scalar>, graphar::IdType,
                   arrow::Scalar::Hash, arrow::Scalar::PtrsEqual>
TableToUnorderedMap(const std::shared_ptr<arrow::Table>& table,
                    const std::string& key_column_name,
                    const std::string& value_column_name);

template <graphar::Type type>
graphar::Status CastToAny(std::shared_ptr<arrow::Array> array, std::any& any,
                          int64_t index);  // NOLINT

template <>
graphar::Status CastToAny<graphar::Type::STRING>(
    std::shared_ptr<arrow::Array> array, std::any& any,
    int64_t index);  // NOLINT

graphar::Status TryToCastToAny(const std::shared_ptr<graphar::DataType>& type,
                               std::shared_ptr<arrow::Array> array,
                               std::any& any, int64_t index = 0);  // NOLINT

class MemUsage {
public:
  MemUsage();
  long GetMaxMemoryUsageInMb() const;
  long GetCurrentMemoryUsageInMb() const;
  void print(bool up = false, bool down = true) const;
private:
  static long GetMaxRssInKBytes();
  static long GetCurrentRssInKBytes();
  long max_memory_usage_, current_memory_usage_;
};

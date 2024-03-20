#pragma once

#include <atomic>

#include "common/logging.hpp"
#include "common/util.hpp"
#include "storage/lsm/buffer.hpp"
#include "storage/lsm/common.hpp"

namespace wing {

namespace lsm {

class ReadFile {
 public:
  ReadFile(const std::string& filename, bool use_direct_io);

  ReadFile(const ReadFile&) = delete;
  ReadFile(ReadFile&&) = delete;
  ReadFile& operator=(const ReadFile&) = delete;
  ReadFile& operator=(ReadFile&&) = delete;

  ~ReadFile();

  ssize_t Read(char* data, size_t n, offset_t offset);
  bool use_direct_io() const { return use_direct_io_; }

 private:
  int fd_;
  std::string filename_;
  bool use_direct_io_;
};

class SeqWriteFile {
 public:
  SeqWriteFile(const std::string& filename, bool use_direct_io);

  ~SeqWriteFile();

  SeqWriteFile(const SeqWriteFile&) = delete;
  SeqWriteFile(SeqWriteFile&&) = delete;
  SeqWriteFile& operator=(const SeqWriteFile&) = delete;
  SeqWriteFile& operator=(SeqWriteFile&&) = delete;

  ssize_t Write(const char* data, size_t n);
  bool use_direct_io() const { return use_direct_io_; }

 private:
  int fd_;
  std::string filename_;
  bool use_direct_io_;
};

class FileWriter {
 public:
  FileWriter(std::unique_ptr<SeqWriteFile> file, size_t buffer_size)
    : file_(std::move(file)),
      buffer_size_(buffer_size),
      buffer_(buffer_size, 4096) {}

  ~FileWriter();

  void Append(const char* data, size_t n);

  template <typename T>
  FileWriter& AppendValue(T x) {
    Append((char*)(&x), sizeof(T));
    return *this;
  }

  FileWriter& AppendString(std::string_view x) {
    Append(x.data(), x.size());
    return *this;
  }

  void Flush();

  size_t size() const { return size_; }

 private:
  std::unique_ptr<SeqWriteFile> file_;
  size_t buffer_size_;
  size_t offset_{0};
  AlignedBuffer buffer_;
  size_t size_{0};
};

class FileReader {
 public:
  FileReader(ReadFile* file, size_t buffer_size, size_t offset)
    : file_(file),
      buffer_size_(buffer_size),
      offset_(offset),
      buffer_(buffer_size, 4096) {}

  ~FileReader() = default;

  /* Read data from the file. */
  void Read(char* data, size_t n);

  /* Set the file pointer to new_offset*/
  void Seek(size_t new_offset);

  template <typename T>
  T ReadValue() {
    T x;
    Read((char*)(&x), sizeof(T));
    return x;
  }

  std::string ReadString(int len) {
    std::string ret(len, 0);
    Read(ret.data(), len);
    return ret;
  }

 private:
  ReadFile* file_;
  size_t buffer_size_;
  size_t offset_{0};
  AlignedBuffer buffer_;
};

class FileNameGenerator {
 public:
  FileNameGenerator(std::string_view prefix, size_t id_begin)
    : prefix_(prefix), id_(id_begin) {}

  std::pair<std::string, size_t> Generate() {
    auto id = id_.fetch_add(1);
    return {fmt::format("{}{}.sst", prefix_, id), id};
  }

  size_t GetID() const { return id_.load(std::memory_order_relaxed); }

 private:
  std::string prefix_;
  std::atomic<size_t> id_{0};
};

}  // namespace lsm

}  // namespace wing

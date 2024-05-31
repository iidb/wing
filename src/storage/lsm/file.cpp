#include "storage/lsm/file.hpp"

#include <fcntl.h>
#include <unistd.h>

#include "common/exception.hpp"
#include "storage/lsm/stats.hpp"

namespace wing {

namespace lsm {

ReadFile::ReadFile(const std::string& filename, bool use_direct_io)
  : filename_(filename), use_direct_io_(use_direct_io) {
  auto flag = O_RDONLY;
#if defined(__linux__)
  if (use_direct_io) {
    flag |= O_DIRECT;
  }
#elif defined(__MINGW64__)
  flag |= O_BINARY;
#endif
  fd_ = ::open(filename.c_str(), flag);
  if (fd_ < 0) {
    throw DBException("::open file {} error! Error: {}", filename, errno);
  }
}

ReadFile::~ReadFile() { ::close(fd_); }

ssize_t ReadFile::Read(char* data, size_t n, offset_t offset) {
#if defined(__linux__)
  ssize_t ret = ::pread(fd_, data, n, offset);
#elif defined(__MINGW64__)
  ::lseek(fd_, offset, SEEK_SET);
  ssize_t ret = ::read(fd_, data, n);
#endif
  GetStatsContext()->total_read_bytes.fetch_add(n, std::memory_order_relaxed);
  if (ret < 0) {
    throw DBException("::pread Error! Error: {}", errno);
  }
  return ret;
}

SeqWriteFile::SeqWriteFile(const std::string& filename, bool use_direct_io)
  : filename_(filename), use_direct_io_(use_direct_io) {
  auto flag = O_WRONLY | O_CREAT | O_TRUNC;
#if defined(__linux__)
  if (use_direct_io) {
    flag |= O_DIRECT;
  }
#elif defined(__MINGW64__)
  flag |= O_BINARY;
#endif
  fd_ = ::open(filename.c_str(), flag, 0644);
  if (fd_ < 0) {
    throw DBException("::open file {} error! Error: {}", filename, errno);
  }
}

SeqWriteFile::~SeqWriteFile() { ::close(fd_); }

ssize_t SeqWriteFile::Write(const char* data, size_t n) {
  ssize_t ret = ::write(fd_, data, n);
  GetStatsContext()->total_write_bytes.fetch_add(n, std::memory_order_relaxed);
  if (ret < 0) {
    throw DBException("::write Error! Error: {}", errno);
  }
  return ret;
}

void FileWriter::Append(const char* data, size_t n) {
  size_t len = std::min(buffer_size_ - offset_, n);
  memcpy(buffer_.data() + offset_, data, len);
  offset_ += len;
  size_ += len;
  if (offset_ == buffer_size_) {
    Flush();
  }
  if (len < n) {
    Append(data + len, n - len);
  }
}

void FileWriter::Flush() {
  file_->Write(buffer_.data(), offset_);
  offset_ = 0;
}

FileWriter::~FileWriter() {
  if (offset_ > 0) {
    Flush();
  }
}

void FileReader::Read(char* data, size_t n) {
  file_->Read(data, n, offset_);
  offset_ += n;
}

void FileReader::Seek(size_t new_offset) { offset_ = new_offset; }

}  // namespace lsm

}  // namespace wing

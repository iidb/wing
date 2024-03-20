#pragma once

#include <cstdlib>

#include "common/logging.hpp"

namespace wing {

namespace lsm {

class AlignedBuffer {
 public:
  AlignedBuffer() = default;

  AlignedBuffer(size_t size, size_t alignment)
    : data_(reinterpret_cast<char *>(aligned_alloc(alignment, size))),
      size_(size) {
    if (!data_) {
      DB_ERR(
          "Error allocating buffer! alignment: {}, size: {}", alignment, size);
    }
  }

  AlignedBuffer(const AlignedBuffer &) = delete;

  AlignedBuffer &operator=(const AlignedBuffer &) = delete;

  AlignedBuffer(AlignedBuffer &&rhs) {
    data_ = rhs.data_;
    size_ = rhs.size_;
    rhs.data_ = nullptr;
    rhs.size_ = 0;
  }

  AlignedBuffer &operator=(AlignedBuffer &&rhs) {
    if (data_) {
      free(data_);
    }
    data_ = rhs.data_;
    size_ = rhs.size_;
    rhs.data_ = nullptr;
    rhs.size_ = 0;
    return *this;
  }

  ~AlignedBuffer() {
    if (data_) {
      free(data_);
    }
  }

  char *data() { return data_; }

  const char *data() const { return data_; }

  size_t size() const { return size_; }

 private:
  char *data_{nullptr};
  size_t size_{0};
};

}  // namespace lsm

}  // namespace wing

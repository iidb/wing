#pragma once

#include <memory>
#include <vector>

namespace wing {

class ArenaAllocator {
 public:
  constexpr static size_t BlockSize = 8192;
  uint8_t* Allocate(size_t size) {
    if (offset_ + size > BlockSize) {
      ptrs_.push_back(
          std::unique_ptr<uint8_t[]>(new uint8_t[std::max(size, BlockSize)]));
      offset_ = 0;
    }
    auto ret = ptrs_.back().get() + offset_;
    offset_ += size;
    return ret;
  }
  void Clear() {
    ptrs_.clear();
    offset_ = BlockSize + 1;
  }

 private:
  std::vector<std::unique_ptr<uint8_t[]>> ptrs_;
  size_t offset_{BlockSize + 1};
};

}  // namespace wing

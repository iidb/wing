#ifndef SAKURA_ALLOCATOR_H__
#define SAKURA_ALLOCATOR_H__

#include <memory>
#include <vector>

namespace wing {

// An allocator providing invariant memory address.
// Used in TupleVector. You can modify it to store on the disk.
template <const size_t BlockSize>
class BlockAllocator {
 public:
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

#endif
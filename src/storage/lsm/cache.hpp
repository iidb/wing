#pragma once

#include <atomic>
#include <functional>
#include <list>
#include <mutex>
#include <unordered_map>

#include "storage/lsm/format.hpp"

namespace wing {

namespace lsm {

struct CacheOptions {
  size_t capacity = 8 * 1024 * 1024;  // 8MiB
};

class CacheKey {
 public:
  CacheKey(uint64_t sstable_id, offset_t offset)
    : sst_id_(sstable_id), offset_(offset) {}

  bool operator==(const CacheKey &rhs) const {
    return sst_id_ == rhs.sst_id_ && offset_ == rhs.offset_;
  }

  struct Hash {
    size_t operator()(const CacheKey &x) const {
      return (x.sst_id_ << 32) | x.offset_;
    }
  };

 private:
  uint64_t sst_id_;
  offset_t offset_;
};

class Cache {
 public:
  class Handle {
   public:
    Handle(const Handle &) = delete;
    Handle &operator=(const Handle &) = delete;
    Handle(Handle &&rhs)
      : cache_(rhs.cache_), block_id_(rhs.block_id_), block_(rhs.block_) {
      rhs.block_ = std::string_view();
    }
    Handle &operator=(Handle &&rhs) {
      this->~Handle();
      cache_ = rhs.cache_;
      block_id_ = rhs.block_id_;
      block_ = rhs.block_;
      rhs.block_ = std::string_view();
      return *this;
    }
    ~Handle() {
      if (block_.data() != nullptr)
        cache_.get().unref_block(block_id_);
    }

    std::string_view block() const { return block_; }

   private:
    Handle(Cache &cache, CacheKey block_id, std::string_view block)
      : cache_(cache), block_id_(block_id), block_(block) {}

    std::reference_wrapper<Cache> cache_;
    CacheKey block_id_;
    std::string_view block_;

    friend class Cache;
  };

  Cache(const CacheOptions &options) : capacity_(options.capacity), size_(0) {}

  std::optional<Cache::Handle> get(uint64_t sstable_id, BlockHandle block);
  Handle insert(uint64_t sstable_id, BlockHandle block, std::string &&content);

 private:
  struct BlockInfo {
    std::string block;
    std::atomic<size_t> refcount;

    BlockInfo(std::string &&b, size_t rc) : block(std::move(b)), refcount(rc) {}
  };

  void unref_block(CacheKey block_id);
  // REQUIRES: this->mu_ held
  void evict();

  const size_t capacity_;

  std::mutex mu_;
  std::unordered_map<CacheKey, BlockInfo, CacheKey::Hash> cache_;
  size_t size_;
  std::unordered_map<CacheKey, std::list<CacheKey>::iterator, CacheKey::Hash>
      lru_map_;
  std::list<CacheKey> lru_list_;

  friend class Block;
};

}  // namespace lsm

}  // namespace wing

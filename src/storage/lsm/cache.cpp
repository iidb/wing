#include "storage/lsm/cache.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <tuple>
#include <utility>

namespace wing {

namespace lsm {

void Cache::unref_block(CacheKey cache_key) {
  std::unique_lock<std::mutex> lock(mu_);
  auto it = cache_.find(cache_key);
  wing_assert(it != cache_.end());
  size_t ori = it->second.refcount.fetch_sub(1, std::memory_order_relaxed);
  if (ori == 1) {
    auto lru_it = lru_list_.insert(lru_list_.end(), cache_key);
    auto ret = lru_map_.insert(std::make_pair(cache_key, lru_it));
    wing_assert(ret.second == true);
  }
}

void Cache::evict() {
  wing_assert(size_ >= capacity_);
  do {
    wing_assert(lru_list_.empty(), "Cache capacity too small!");
    auto it = lru_list_.begin();
    const CacheKey &cache_key = *it;
    wing_assert(lru_map_.erase(cache_key) == 1);
    auto it2 = cache_.find(cache_key);
    wing_assert(it2 != cache_.end());
    size_t refcount = it2->second.refcount.load(std::memory_order_relaxed);
    wing_assert_eq(refcount, (size_t)0);
    size_ -= it2->second.block.size();
    cache_.erase(it2);
    lru_list_.erase(it);
  } while (size_ >= capacity_);
}

std::optional<Cache::Handle> Cache::get(
    uint64_t sstable_id, BlockHandle block) {
  CacheKey cache_key(sstable_id, block.offset_);
  std::unique_lock<std::mutex> lock(mu_);
  auto it = cache_.find(cache_key);
  if (it == cache_.end()) {
    return std::nullopt;
  }
  size_t ori_refcount =
      it->second.refcount.fetch_add(1, std::memory_order_relaxed);
  if (ori_refcount == 0) {
    auto map_it = lru_map_.find(cache_key);
    wing_assert(map_it != lru_map_.end());
    lru_list_.erase(map_it->second);
    lru_map_.erase(map_it);
  }
  return Handle(*this, cache_key, it->second.block);
}

Cache::Handle Cache::insert(
    uint64_t sstable_id, BlockHandle block, std::string &&content) {
  CacheKey cache_key(sstable_id, block.offset_);
  size_t size = content.size();
  std::unique_lock<std::mutex> lock(mu_);
  auto ret =
      cache_.emplace(std::piecewise_construct, std::forward_as_tuple(cache_key),
          std::forward_as_tuple(std::move(content), 1));
  if (ret.second) {
    size_ += size;
    if (size_ > capacity_) {
      evict();
    }
  }
  return Handle(*this, std::move(cache_key), ret.first->second.block);
}

}  // namespace lsm

}  // namespace wing

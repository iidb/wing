#ifndef SAKURA_GEN_PK_H__
#define SAKURA_GEN_PK_H__

#include <atomic>
#include <string>

namespace wing {

/**
 * Use to generate primary key when multiple txns are inserting one table.
 */
class GenPKHandle {
 public:
  GenPKHandle(std::atomic<int64_t>* pk) : pk_(pk) {}
  int64_t Gen() { return pk_->fetch_add(1, std::memory_order::relaxed); }
  operator bool() const { return pk_ != nullptr; }

 private:
  std::atomic<int64_t>* pk_;
};

}  // namespace wing

#endif
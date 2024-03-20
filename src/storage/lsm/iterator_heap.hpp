#pragma once

#include <queue>

#include "storage/lsm/format.hpp"
#include "storage/lsm/iterator.hpp"

namespace wing {

namespace lsm {

template <typename T>
class IteratorHeap final : public Iterator {
 public:
  IteratorHeap() = default;

  void Push(T* it) { DB_ERR("Not implemented!"); }

  void Build() { DB_ERR("Not implemented!"); }

  bool Valid() override { DB_ERR("Not implemented!"); }

  Slice key() override { DB_ERR("Not implemented!"); }

  Slice value() override { DB_ERR("Not implemented!"); }

  void Next() override { DB_ERR("Not implemented!"); }

 private:
};

}  // namespace lsm

}  // namespace wing

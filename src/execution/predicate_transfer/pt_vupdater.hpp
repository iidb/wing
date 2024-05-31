#pragma once
#include "execution/executor.hpp"

namespace wing {

class PtVecUpdater {
 public:
  PtVecUpdater(std::unique_ptr<VecExecutor> input, size_t num_cols)
    : input_(std::move(input)), num_cols_(num_cols) {}

  void Execute(
      const std::vector<std::string>& bloom_filter, BitVector& valid_bits);

 private:
  /* The input of updated table */
  std::unique_ptr<VecExecutor> input_;
  /* The number of columns */
  size_t num_cols_;
};

}  // namespace wing

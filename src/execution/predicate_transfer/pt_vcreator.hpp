#pragma once

#include "execution/executor.hpp"

namespace wing {

class PtVecCreator {
 public:
  PtVecCreator(size_t bloom_bit_per_key_n, std::unique_ptr<VecExecutor> input,
      size_t num_cols)
    : bloom_bit_per_key_n_(bloom_bit_per_key_n),
      input_(std::move(input)),
      num_cols_(num_cols) {}

  void Execute();

  const std::vector<std::string>& GetResult() const { return result_; }

  std::vector<std::string>& GetResult() { return result_; }

 private:
  /* number of bloom bits per each key in bloom filter */
  size_t bloom_bit_per_key_n_;
  /* The executor */
  std::unique_ptr<VecExecutor> input_;
  /* The number of output columns */
  size_t num_cols_;
  /* The result bloom filter */
  std::vector<std::string> result_;
};

}  // namespace wing

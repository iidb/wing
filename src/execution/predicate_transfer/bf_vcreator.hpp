#pragma once

#include "execution/executor.hpp"

namespace wing {

class BfVecCreator {
 public:
  BfVecCreator(size_t bloom_bit_per_key_n, std::unique_ptr<VecExecutor> input)
    : bloom_bit_per_key_n_(bloom_bit_per_key_n), input_(std::move(input)) {}

  void Execute();

  const std::string& GetResult() const { return result_; }

 private:
  size_t bloom_bit_per_key_n_;
  std::unique_ptr<VecExecutor> input_;
  std::string result_;
};

}  // namespace wing

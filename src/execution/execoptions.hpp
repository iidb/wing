#pragma once
#include <memory>

namespace wing {

class ExecOptions {
 public:
  /* The maximum size of batch in vectorized executor */
  size_t max_batch_size{1024};

  /* The style of executor, can be 'vec' (vectorized pull-based), 'volcano'
   * (tuple-at-a-time pull-based), 'jit' (push-based using JIT)*/
  std::string style{"vec"};

  /* If predicate transfer is enabled, then there will be an additional stage
   * before execution to calculate predicate transfer information. */
  bool enable_predicate_transfer{false};

  /* Number of bits per key in bloom filters in predicate transfer*/
  size_t pt_bits_per_key_n{20};
};

}  // namespace wing

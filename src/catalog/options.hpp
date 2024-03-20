#pragma once

#include <memory>

namespace wing {

class WingOptions {
 public:
  /* The size of batch in vectorized execution engine. */
  size_t size_batch{1024};

  /* Enable vectorized execution engine or not. */
  bool enable_vec_exec{false};

  /* Enable JIT execution engine or not. */
  bool enable_jit_exec{false};

  /* Whether we print the message of DBException in wing::Instance. */
  bool print_exception_msg{true};

  /* Whether we strictly check if the chunk size <= size_batch. */
  bool check_chunk_size{false};
};

}  // namespace wing

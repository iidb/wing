#pragma once
#include <memory>

namespace wing {

class ExecOptions {
 public:
  /* The maximum size of batch in vectorized executor */
  size_t max_batch_size{1024};
};

}  // namespace wing

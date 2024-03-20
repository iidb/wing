#pragma once

#include <memory>

namespace wing {

namespace lsm {

enum class GetResult : uint8_t {
  kFound = 0,
  kNotFound,
  kDelete,
};

using offset_t = uint32_t;
using seq_t = uint64_t;

}  // namespace lsm

}  // namespace wing

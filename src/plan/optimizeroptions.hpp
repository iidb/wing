#pragma once

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

namespace wing {

class OptimizerOptions {
 public:
  double scan_cost{0.001};

  double hash_join_cost{0.01};

  std::optional<std::vector<std::pair<std::vector<std::string>, double>>>
      true_cardinality_hints;
};

}  // namespace wing

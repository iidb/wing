#include "storage/lsm/stats.hpp"

namespace wing {

namespace lsm {

StatsContext* GetStatsContext() {
  static StatsContext context;
  return &context;
}

}  // namespace lsm

}  // namespace wing

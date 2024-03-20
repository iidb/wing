#pragma once

#include <atomic>

namespace wing {

namespace lsm {

struct StatsContext {
  /* Total bytes of all read operations */
  std::atomic<uint64_t> total_read_bytes{0};
  /* Total bytes of all write operations */
  std::atomic<uint64_t> total_write_bytes{0};
  /* Total bytes of flushed MemTable */
  std::atomic<uint64_t> total_input_bytes{0};
};

StatsContext* GetStatsContext();

}  // namespace lsm

}  // namespace wing

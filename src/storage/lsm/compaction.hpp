#pragma once

#include "storage/lsm/level.hpp"
#include "storage/lsm/sst.hpp"

namespace wing {

namespace lsm {

class Compaction {
 public:
  Compaction(std::vector<std::shared_ptr<SSTable>> input_ssts,
      std::vector<std::shared_ptr<SortedRun>> input_runs, int src_level,
      int target_level, std::shared_ptr<SortedRun> target_sorted_run,
      bool is_trivial_move)
    : input_ssts_(std::move(input_ssts)),
      input_runs_(std::move(input_runs)),
      src_level_(src_level),
      target_level_(target_level),
      target_sorted_run_(target_sorted_run),
      is_trivial_move_(is_trivial_move) {}

  std::shared_ptr<SortedRun> target_sorted_run() const {
    return target_sorted_run_;
  }

  const std::vector<std::shared_ptr<SSTable>>& input_ssts() const {
    return input_ssts_;
  }

  const std::vector<std::shared_ptr<SortedRun>>& input_runs() const {
    return input_runs_;
  }

  int target_level() const { return target_level_; }

  int src_level() const { return src_level_; }

 private:
  /* The input SSTables */
  std::vector<std::shared_ptr<SSTable>> input_ssts_;
  /* The input sorted runs. */
  std::vector<std::shared_ptr<SortedRun>> input_runs_;
  /**
   * The source level and the target level
   * Basically, a compaction picks files in the source level
   * and compacts them to the target level.
   * */
  int src_level_{-1}, target_level_{-1};
  /**
   * The sorted run that is merged with.
   * It can be null, which means it is not merged with a sorted run
   * (For example in tiering compaction strategy)
   */
  std::shared_ptr<SortedRun> target_sorted_run_;
  /**
   * Whether it is a trivial move.
   * If it is a trivial move, then we can simply move the pointer of the SSTable
   * to the target level
   * */
  bool is_trivial_move_{false};
};

}  // namespace lsm

}  // namespace wing

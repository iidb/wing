#pragma once

#include "storage/lsm/compaction.hpp"
#include "storage/lsm/sst.hpp"
#include "storage/lsm/version.hpp"

namespace wing {

namespace lsm {

class CompactionPicker {
 public:
  virtual std::unique_ptr<Compaction> Get(Version* version) = 0;

  virtual ~CompactionPicker() = default;
};

class LeveledCompactionPicker final : public CompactionPicker {
 public:
  LeveledCompactionPicker(
      size_t ratio, size_t base_level_size, size_t level0_compaction_trigger)
    : ratio_(ratio),
      base_level_size_(base_level_size),
      level0_compaction_trigger_(level0_compaction_trigger) {}

  std::unique_ptr<Compaction> Get(Version* version) override;

 private:
  /* The target size ratio */
  size_t ratio_{10};
  /* The base size levels */
  size_t base_level_size_{0};
  /* The maximum amount of sorted runs in Level 0 */
  size_t level0_compaction_trigger_{0};
};

class TieredCompactionPicker final : public CompactionPicker {
 public:
  TieredCompactionPicker(
      size_t ratio, size_t base_level_size, size_t level0_compaction_trigger)
    : ratio_(ratio),
      base_level_size_(base_level_size),
      level0_compaction_trigger_(level0_compaction_trigger) {}

  std::unique_ptr<Compaction> Get(Version* version) override;

 private:
  /* The target size ratio */
  size_t ratio_{10};
  /* The base size levels */
  size_t base_level_size_{0};
  /* The maximum amount of sorted runs in Level 0 */
  size_t level0_compaction_trigger_{0};
};

class LazyLevelingCompactionPicker final : public CompactionPicker {
 public:
  LazyLevelingCompactionPicker(
      size_t ratio, size_t base_level_size, size_t level0_compaction_trigger)
    : ratio_(ratio),
      base_level_size_(base_level_size),
      level0_compaction_trigger_(level0_compaction_trigger) {}

  std::unique_ptr<Compaction> Get(Version* version) override;

 private:
  /* The target size ratio */
  size_t ratio_{10};
  /* The base size levels */
  size_t base_level_size_{0};
  /* The maximum amount of sorted runs in Level 0 */
  size_t level0_compaction_trigger_{0};
};

class FluidCompactionPicker final : public CompactionPicker {
 public:
  FluidCompactionPicker(double alpha, double scan_length,
      size_t base_level_size, size_t level0_compaction_trigger)
    : alpha_(alpha),
      scan_length_(scan_length),
      base_level_size_(base_level_size),
      level0_compaction_trigger_(level0_compaction_trigger) {}

  std::unique_ptr<Compaction> Get(Version* version) override;

 private:
  /* the target alpha */
  double alpha_{0};
  /* the target scan length */
  double scan_length_{0};
  /* The base size levels */
  size_t base_level_size_{0};
  /* The maximum amount of sorted runs in Level 0 */
  size_t level0_compaction_trigger_{0};
};

}  // namespace lsm

}  // namespace wing

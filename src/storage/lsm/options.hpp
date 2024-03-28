#pragma once

#include <filesystem>

#include "storage/lsm/cache.hpp"

namespace wing {

namespace lsm {

struct Options {
  /* The directory path of the database */
  std::filesystem::path db_path;
  /* The target size of SSTable */
  uint64_t sst_file_size = 64 * 1024 * 1024;
  /* The target size of data block in SSTable */
  size_t block_size = 4 * 1024;
  /* The size of write buffer */
  size_t write_buffer_size = 1024 * 1024;
  /* Use O_DIRECT or not */
  bool use_direct_io = false;
  /* Use bloom filter or not*/
  bool enable_bloom_filter = true;
  /* Whether we create a new database in the directory */
  bool create_new = true;
  /* The maximum number of immutable MemTables. */
  size_t max_immutable_count = 4;
  /* The name of compaction strategy. */
  std::string compaction_strategy_name = "tiered";
  /* The minimum number of sorted runs for triggering compaction in Level 0*/
  size_t level0_compaction_trigger = 4;
  /**
   * The maximum number of sorted runs in Level 0.
   * It stops writes when the number of sorted runs reaches this limit.
   */
  size_t level0_stop_writes_trigger = 20;
  /* The default size ratio used in tiering/leveling compaction strategy. */
  size_t compaction_size_ratio = 10;
  /* The number of bits per key in bloom filter, by default */
  size_t bloom_bits_per_key = 10;
  CacheOptions cache{};
};

}  // namespace lsm

}  // namespace wing

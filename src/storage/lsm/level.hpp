#pragma once

#include "storage/lsm/sst.hpp"

namespace wing {

namespace lsm {

class SortedRunIterator;

class SortedRun {
 public:
  SortedRun(
      const std::vector<SSTInfo>& ssts, size_t block_size, bool use_direct_io)
    : block_size_(block_size), use_direct_io_(use_direct_io) {
    size_ = 0;
    for (auto& sst : ssts) {
      ssts_.push_back(
          std::make_shared<SSTable>(sst, block_size_, use_direct_io_));
      size_ += sst.size_;
    }
  }

  SortedRun(std::vector<std::shared_ptr<SSTable>> ssts, size_t block_size,
      bool use_direct_io)
    : block_size_(block_size), use_direct_io_(use_direct_io) {
    size_ = 0;
    ssts_ = std::move(ssts);
    for (auto& sst : ssts_) {
      size_ += sst->GetSSTInfo().size_;
    }
  }

  ~SortedRun();

  /**
   * Try to get the associated value of key with the sequence number <= seq.
   * If the record has type RecordType::Value, then it copies the value,
   * and returns GetResult::kFound
   * If the record has type RecordType::Deletion, then it does nothing to the
   * value, and returns GetResult::kDelete If there is no such record, it
   * returns GetResult::kNotFound.
   * */
  GetResult Get(Slice key, uint64_t seq, std::string* value);

  /* Return an iterator positioned at the first record that is not smaller than
   * (key, seq). */
  SortedRunIterator Seek(Slice key, uint64_t seq);

  /* Return an iterator positioned at the beginning of the SSTable */
  SortedRunIterator Begin();

  /* Get the number of SSTables. */
  size_t SSTCount() const { return ssts_.size(); }

  const std::vector<std::shared_ptr<SSTable>>& GetSSTs() const { return ssts_; }

  void SetCompactionInProcess(bool compaction_in_process) {
    compaction_in_process_ = compaction_in_process;
  }

  bool GetCompactionInProcess() const { return compaction_in_process_; }

  /* Get the total size of SSTables. */
  size_t size() const { return size_; }

  size_t block_size() const { return block_size_; }

  bool use_direct_io() const { return use_direct_io_; }

  ParsedKey GetLargestKey() const { return ssts_.back()->GetLargestKey(); }

  ParsedKey GetSmallestKey() const { return ssts_.front()->GetSmallestKey(); }

  void SetRemoveTag(bool remove_tag) { remove_tag_ = remove_tag; }

  bool GetRemoveTag() const { return remove_tag_; }

 private:
  /* The SSTables. */
  std::vector<std::shared_ptr<SSTable>> ssts_;
  /* The total size of the sorted run. */
  size_t size_;
  /* The size of a data block. */
  size_t block_size_;
  /* Whether it uses direct io. */
  bool use_direct_io_;
  /* If it is picked as an input of a compaction task. */
  bool compaction_in_process_{false};
  /* If it is true, the whole sorted run is removed in deconstruction. */
  bool remove_tag_{false};

  friend class SortedRunIterator;
};

class SortedRunIterator final : public Iterator {
 public:
  SortedRunIterator() = default;

  SortedRunIterator(SortedRun* run, SSTableIterator sst_it, int sst_id)
    : run_(run), sst_it_(std::move(sst_it)), sst_id_(sst_id) {}

  void SeekToFirst();

  bool Valid() override;

  Slice key() override;

  Slice value() override;

  void Next() override;

 private:
  /* The referenced sorted run */
  SortedRun* run_;
  /* The SSTable iterator of the current SSTable */
  SSTableIterator sst_it_;
  /* The index of the current SSTable */
  size_t sst_id_{0};
};

class Level {
 public:
  Level(int level_id) : level_id_(level_id) {}

  Level(int level_id, std::vector<std::shared_ptr<SortedRun>> runs)
    : level_id_(level_id), runs_(std::move(runs)) {
    size_ = 0;
    for (auto& run : runs_) {
      size_ += run->size();
    }
  }

  const std::vector<std::shared_ptr<SortedRun>>& GetRuns() const {
    return runs_;
  }

  GetResult Get(Slice key, uint64_t seq, std::string* value);

  /* Get the level id */
  int GetID() const { return level_id_; }

  /* Append several sorted runs to the level. */
  void Append(std::vector<std::shared_ptr<SortedRun>> runs);

  /* Append a sorted run to the level. */
  void Append(std::shared_ptr<SortedRun> run);

  /* The total size of the sorted runs. */
  size_t size() const { return size_; }

 private:
  /* The id of the level */
  int level_id_;
  /* The total size of all the sorted runs in the level */
  size_t size_{0};
  /* Sorted runs. */
  std::vector<std::shared_ptr<SortedRun>> runs_;
};

}  // namespace lsm

}  // namespace wing

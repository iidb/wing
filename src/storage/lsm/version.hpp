#pragma once

#include "storage/lsm/common.hpp"
#include "storage/lsm/iterator_heap.hpp"
#include "storage/lsm/level.hpp"
#include "storage/lsm/memtable.hpp"
#include "storage/lsm/sst.hpp"

namespace wing {

namespace lsm {

class Version {
 public:
  Version(std::vector<Level>&& levels) : levels_(std::move(levels)) {}

  Version() = default;

  // Return true if the GetResult is kFound
  // Otherwise return false
  bool Get(Slice user_key, seq_t seq, std::string* value);

  const std::vector<Level>& GetLevels() const { return levels_; }

  /**
   * Append sorted runs to the Level level_id
   * It will create new levels if level_id >= levels_.size()
   * */
  void Append(
      uint32_t level_id, std::vector<std::shared_ptr<SortedRun>> sorted_runs);

  /**
   * Append a sorted run to the Level level_id
   * It will create new levels if level_id >= levels_.size()
   * */
  void Append(uint32_t level_id, std::shared_ptr<SortedRun> sorted_run);

 private:
  std::vector<Level> levels_;
};

class SuperVersionIterator;

class SuperVersion {
 public:
  SuperVersion(std::shared_ptr<MemTable> mt,
      std::shared_ptr<std::vector<std::shared_ptr<MemTable>>> imms,
      std::shared_ptr<Version> version)
    : mt_(std::move(mt)),
      imms_(std::move(imms)),
      version_(std::move(version)) {}

  std::shared_ptr<MemTable> GetMt() const { return mt_; }
  std::shared_ptr<std::vector<std::shared_ptr<MemTable>>> GetImms() const {
    return imms_;
  }
  std::shared_ptr<Version> GetVersion() const { return version_; }

  // Return true if the GetResult is kFound
  // Otherwise return false
  bool Get(Slice user_key, seq_t seq, std::string* value);

  std::string ToString() const;

 private:
  std::shared_ptr<MemTable> mt_;
  std::shared_ptr<std::vector<std::shared_ptr<MemTable>>> imms_;
  std::shared_ptr<Version> version_;

  friend class SuperVersionIterator;
};

class SuperVersionIterator final : public Iterator {
 public:
  SuperVersionIterator(SuperVersion* sv) : sv_(sv) {}

  /* Move the the beginning */
  void SeekToFirst();

  /* Find the first record >= (user_key, seq) */
  void Seek(Slice key, seq_t seq);

  bool Valid() override;

  Slice key() override;

  Slice value() override;

  void Next() override;

 private:
  /* The referenced superversion */
  SuperVersion* sv_;
  /* The iterators */
  IteratorHeap<Iterator> it_;
  /* The memtable iterators */
  std::vector<MemTableIterator> mt_its_;
  /* The sorted run iterators */
  std::vector<SortedRunIterator> sst_its_;
};

}  // namespace lsm

}  // namespace wing

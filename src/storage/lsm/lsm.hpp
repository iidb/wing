#pragma once

#include <cassert>
#include <condition_variable>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <thread>
#include <utility>
#include <variant>

#include "storage/lsm/cache.hpp"
#include "storage/lsm/compaction_pick.hpp"
#include "storage/lsm/memtable.hpp"
#include "storage/lsm/options.hpp"
#include "storage/lsm/version.hpp"

namespace wing {

namespace lsm {

class DBIterator;

class DBImpl {
 public:
  DBImpl(const Options &options);
  ~DBImpl();

  static std::unique_ptr<DBImpl> Create(const Options &options) {
    return std::make_unique<DBImpl>(options);
  }

  void Put(Slice key, Slice value);
  void Del(Slice key);
  // Return true if kFound, false if not
  bool Get(Slice key, std::string *value);
  void Save();
  void FlushAll();
  void WaitForFlushAndCompaction();
  size_t CurrentSeq() const { return seq_; }

  DBIterator Begin();
  DBIterator Seek(Slice key);
  std::shared_ptr<SuperVersion> GetSV();
  const Options &GetOptions() const { return options_; }

 private:
  void SwitchMemtable(bool force = false);
  void FlushThread();
  void CompactionThread();
  std::vector<std::shared_ptr<MemTable>> PickMemTables();
  void InstallSV(std::shared_ptr<SuperVersion> sv);
  void SaveMetadata();
  void LoadMetadata();

  // Require: DB Mutex held
  void StopWrite();

  Options options_;
  Cache cache_;
  size_t seq_;

  std::vector<std::thread> threads_;
  std::condition_variable flush_cv_;
  std::condition_variable compact_cv_;
  bool stop_signal_{false};
  bool compact_flag_{false};
  bool flush_flag_{false};

  std::mutex write_mutex_;
  std::mutex db_mutex_;
  std::shared_mutex sv_mutex_;
  std::shared_ptr<SuperVersion> sv_;
  std::unique_ptr<FileNameGenerator> filename_gen_;
  std::unique_ptr<CompactionPicker> compaction_picker_;
};

class DBIterator final : public Iterator {
 public:
  DBIterator(std::shared_ptr<SuperVersion> sv, seq_t seq)
    : sv_(std::move(sv)), it_(sv_.get()), seq_(seq) {}

  void SeekToFirst();

  void Seek(Slice key);

  bool Valid() override;

  Slice key() const override;

  Slice value() const override;

  void Next() override;

 private:
  std::shared_ptr<SuperVersion> sv_;
  SuperVersionIterator it_;
  seq_t seq_;
  InternalKey current_key_;
};

}  // namespace lsm

}  // namespace wing

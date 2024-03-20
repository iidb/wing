#pragma once

#include <map>
#include <shared_mutex>
#include <string>

#include "common/allocator.hpp"
#include "storage/lsm/common.hpp"
#include "storage/lsm/format.hpp"
#include "storage/lsm/iterator.hpp"
#include "storage/lsm/options.hpp"

namespace wing {

namespace lsm {

class MemTableIterator;

class MemTable {
 public:
  MemTable() : size_(0) {}

  void Put(Slice user_key, seq_t seq, Slice value);

  void Del(Slice user_key, seq_t seq);

  /* Find a record with the same key and the largest sequence number <= seq */
  GetResult Get(Slice user_key, seq_t seq, std::string* value);

  size_t size() const { return size_; }

  std::map<ParsedKey, Slice>& GetTable() { return table_; }

  MemTableIterator Seek(Slice user_key, seq_t seq);

  MemTableIterator Begin();

  void SetFlushInProgress(bool in_process) { flush_in_progress_ = in_process; }

  bool GetFlushInProgress() const { return flush_in_progress_; }

  void SetFlushComplete(bool complete) { flush_complete_ = complete; }

  bool GetFlushComplete() const { return flush_complete_; }

 private:
  void Add(ParsedKey key, Slice value);

  std::shared_mutex mu_;
  std::map<ParsedKey, Slice> table_;
  uint64_t size_;
  ArenaAllocator alloc_;
  bool flush_in_progress_{false};
  bool flush_complete_{false};

  friend class MemTableIterator;
};

class MemTableIterator final : public Iterator {
 public:
  MemTableIterator(MemTable* table) : table_(table) {}

  void Seek(Slice key, seq_t seq) {
    it_ = table_->table_.lower_bound(ParsedKey(key, seq, RecordType::Value));
  }

  void SeekToFirst() { it_ = table_->table_.begin(); }

  bool Valid() override { return it_ != table_->table_.end(); }

  Slice key() override {
    return Slice(it_->first.user_key_.data(), it_->first.size());
  }

  Slice value() override { return it_->second; }

  void Next() override { it_++; }

 private:
  MemTable* table_;
  std::map<ParsedKey, Slice>::iterator it_;
};

}  // namespace lsm

}  // namespace wing

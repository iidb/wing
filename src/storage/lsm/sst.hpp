#pragma once

#include <string>
#include <vector>

#include "storage/lsm/block.hpp"
#include "storage/lsm/cache.hpp"
#include "storage/lsm/common.hpp"
#include "storage/lsm/file.hpp"
#include "storage/lsm/format.hpp"
#include "storage/lsm/iterator.hpp"
#include "storage/lsm/options.hpp"

namespace wing {

namespace lsm {

class SSTableIterator;

class SSTable {
 public:
  /**
   * id: the SSTable ID
   * filename: the name of the SSTable file
   * index_offset: the offset of the index block in SSTable
   * -----------------------
   * Below are global options (see lsm/options.hpp):
   * block_size: The size of data block in the SSTable
   * use_direct_io: Enable O_DIRECT or not.
   */
  SSTable(SSTInfo sst_info, size_t block_size, bool use_direct_io);

  ~SSTable();

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
  SSTableIterator Seek(Slice key, uint64_t seq);

  /* Return an iterator positioned at the beginning of the SSTable */
  SSTableIterator Begin();

  /* The largest key of the SSTable. */
  ParsedKey GetLargestKey() const { return largest_key_; }

  /* The smallest key of the SSTable. */
  ParsedKey GetSmallestKey() const { return smallest_key_; }

  void SetCompactionInProcess(bool compaction_in_process) {
    compaction_in_process_ = compaction_in_process;
  }

  bool GetCompactionInProcess() const { return compaction_in_process_; }

  void SetRemoveTag(bool remove_tag) { remove_tag_ = remove_tag; }

  bool GetRemoveTag() const { return remove_tag_; }

  const SSTInfo& GetSSTInfo() const { return sst_info_; }

 private:
  int FindBlock(Slice key, uint64_t seq) const;

  /* The information of SSTable. */
  SSTInfo sst_info_;
  /* The file manager. */
  std::unique_ptr<ReadFile> file_;
  /* The index data, which is initialized in construction. */
  std::vector<IndexValue> index_;
  /* The block size of the data block. */
  size_t block_size_;
  /* The key range of the SSTable, which is initialized in construction. */
  InternalKey smallest_key_, largest_key_;
  /* If it is picked as an input of a compaction task. */
  bool compaction_in_process_{false};
  /* If it is true, then the SSTable file will be removed in deconstrution. */
  bool remove_tag_{false};

  friend class SSTableIterator;
};

class SSTableIterator final : public Iterator {
 public:
  SSTableIterator() = default;

  SSTableIterator(SSTable* sst) : sst_(sst), buf_(sst->block_size_, 4096) {}

  void SeekToFirst();

  void Seek(Slice key, uint64_t seq);

  bool Valid() override;

  Slice key() override;

  Slice value() override;

  void Next() override;

 private:
  void ReadBlock();

  SSTable* sst_{nullptr};
  size_t block_id_{0};
  BlockIterator block_it_;
  AlignedBuffer buf_;
};

class SSTableBuilder {
 public:
  SSTableBuilder(std::unique_ptr<FileWriter> writer, size_t block_size)
    : writer_(std::move(writer)), block_builder_(block_size, writer_.get()) {}

  ~SSTableBuilder() = default;

  void Append(ParsedKey key, Slice value);

  void Finish();

  std::vector<IndexValue> GetIndexData() const { return index_data_; }

  ParsedKey GetLargestKey() const { return largest_key_; }

  ParsedKey GetSmallestKey() const { return smallest_key_; }

  size_t size() const { return writer_->size(); }

  size_t count() const { return count_; }

  size_t GetIndexOffset() const { return index_offset_; }

 private:
  /* The file writer */
  std::unique_ptr<FileWriter> writer_;
  /* The builder for the data block */
  BlockBuilder block_builder_;
  /* The index data */
  std::vector<IndexValue> index_data_;
  /* The index offset */
  size_t index_offset_{0};
  /* The key range of the SSTable */
  InternalKey largest_key_, smallest_key_;
  /* The number of records in this SSTable. */
  size_t count_{0};
};

}  // namespace lsm

}  // namespace wing

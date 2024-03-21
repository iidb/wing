#pragma once

#include <string>

#include "storage/lsm/common.hpp"
#include "storage/lsm/file.hpp"

namespace wing {

namespace lsm {

using Slice = std::string_view;

using PinnableSlice = std::string;

enum class RecordType : uint8_t {
  Deletion = 0,
  Value,
};

class ParsedKey;

// A data structure which stores a record in memory.
class InternalKey {
 public:
  InternalKey() : InternalKey("", 0, RecordType::Value) {}

  InternalKey(Slice user_key, seq_t seq, RecordType type) {
    rep_.append(user_key);
    rep_.append(reinterpret_cast<char*>(&seq), sizeof(seq_t));
    rep_.append(reinterpret_cast<char*>(&type), sizeof(type));
  }

  InternalKey(Slice key) { rep_ = key; }

  InternalKey& operator=(Slice key) {
    rep_ = key;
    return *this;
  }

  InternalKey(ParsedKey key);

  Slice user_key() const {
    return Slice(
        rep_.data(), rep_.length() - sizeof(RecordType) - sizeof(seq_t));
  }

  seq_t seq() const {
    return *reinterpret_cast<const seq_t*>(
        rep_.data() + rep_.size() - sizeof(seq_t) - sizeof(RecordType));
  }

  RecordType record_type() const {
    return *reinterpret_cast<const RecordType*>(
        rep_.data() + rep_.length() - sizeof(RecordType));
  }

  Slice GetSlice() const { return Slice(rep_.data(), rep_.size()); }

  /* The size of the record. */
  size_t size() const { return rep_.size(); }

 private:
  PinnableSlice rep_;
};

// A data structure which represents a record but it only stores a reference
struct ParsedKey {
 public:
  ParsedKey() : ParsedKey(Slice(), 0, RecordType::Deletion) {}

  ParsedKey(Slice ikey) {
    user_key_ =
        Slice(ikey.data(), ikey.size() - sizeof(RecordType) - sizeof(seq_t));
    seq_ = *reinterpret_cast<const seq_t*>(
        ikey.data() + ikey.size() - sizeof(seq_t) - sizeof(RecordType));
    type_ = *reinterpret_cast<const RecordType*>(
        ikey.data() + ikey.size() - sizeof(RecordType));
  }

  ParsedKey(Slice user_key, seq_t seq, RecordType type)
    : user_key_(user_key), seq_(seq), type_(type) {}

  ParsedKey(const InternalKey& ikey) : ParsedKey(ikey.GetSlice()) {}

  /* The size of the record. */
  size_t size() const {
    return user_key_.size() + sizeof(seq_) + sizeof(type_);
  }

  std::strong_ordering operator<=>(const ParsedKey& pk) const {
    auto key_result = user_key_ <=> pk.user_key_;
    return key_result == 0 ? pk.seq_ <=> seq_ : key_result;
  }

  Slice user_key_;
  seq_t seq_;
  RecordType type_;
};

inline InternalKey::InternalKey(ParsedKey key)
  : InternalKey(key.user_key_, key.seq_, key.type_) {}

struct BlockHandle {
  /* The offset of the block. */
  offset_t offset_;
  /* The size of the block. */
  offset_t size_;
  /* The number of entries in the block. */
  offset_t count_;
};

struct IndexValue {
  /* The largest key in the corresponding data block. */
  InternalKey key_;
  /* The handle of the data block. */
  BlockHandle block_;
};

struct SSTInfo {
  /* The size of the SSTable */
  size_t size_;
  /* The number of records in the SSTable */
  size_t count_;
  /* The ID of the SSTable */
  size_t sst_id_;
  /* The offset of the index block */
  size_t index_offset_;
  /* The path of the SSTable */
  std::string filename_;
};

}  // namespace lsm

}  // namespace wing

#include "storage/lsm/memtable.hpp"

#include "common/logging.hpp"
#include "common/serializer.hpp"

namespace wing {

namespace lsm {

void MemTable::Add(ParsedKey key, Slice value) {
  auto ptr = (char *)alloc_.Allocate(key.size() + value.size());
  utils::Serializer(ptr)
      .WriteString(key.user_key_)
      .Write(key.seq_)
      .Write(key.type_)
      .WriteString(value);
  size_ += key.size() + value.size() + sizeof(offset_t) * 2;
  auto parsed_key =
      ParsedKey(Slice(ptr, key.user_key_.size()), key.seq_, key.type_);
  auto copied_value = Slice(ptr + key.size(), value.size());
  table_.emplace(parsed_key, copied_value);
}

void MemTable::Put(Slice user_key, seq_t seq, Slice value) {
  std::unique_lock<std::shared_mutex> lck(mu_);
  Add(ParsedKey(user_key, seq, RecordType::Value), value);
}

void MemTable::Del(Slice user_key, seq_t seq) {
  std::unique_lock<std::shared_mutex> lck(mu_);
  Add(ParsedKey(user_key, seq, RecordType::Deletion), Slice());
}

void MemTable::Clear() {
  std::unique_lock<std::shared_mutex> lck(mu_);
  table_.clear();
}

GetResult MemTable::Get(Slice user_key, seq_t seq, std::string *value) {
  std::shared_lock<std::shared_mutex> lock(mu_);
  auto it = table_.lower_bound(ParsedKey(user_key, seq, RecordType::Value));
  if (it == table_.end() || it->first.user_key_ != user_key) {
    return GetResult::kNotFound;
  } else {
    switch (it->first.type_) {
      case RecordType::Deletion:
        return GetResult::kDelete;
      case RecordType::Value:
        *value = it->second;
        return GetResult::kFound;
    }
  }
  DB_ERR("Incorrect key value!");
}

MemTableIterator MemTable::Seek(Slice user_key, seq_t seq) {
  MemTableIterator it(this);
  it.Seek(user_key, seq);
  return it;
}

MemTableIterator MemTable::Begin() {
  MemTableIterator it(this);
  it.SeekToFirst();
  return it;
}

}  // namespace lsm

}  // namespace wing

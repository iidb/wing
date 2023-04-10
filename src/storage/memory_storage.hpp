#ifndef SAKURA_MEMORY_STORAGE_H__
#define SAKURA_MEMORY_STORAGE_H__

// This file is not allowed to be used in submissions.
// We disable this file when grading to prevent unintentional use of this file.
#ifndef SAKURA_ONLINE_JUDGE

#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>

#include "catalog/schema.hpp"
#include "common/allocator.hpp"
#include "storage.hpp"

namespace wing {

/**
 * Use std::map to maintain valid tuples.
 */
class MemoryTable {
 private:
  typedef std::map<std::string, std::string, std::less<>> map_t;

 public:
  class Iterator : public wing::Iterator<const uint8_t*> {
   public:
    Iterator(MemoryTable::map_t::const_iterator iter_begin,
        MemoryTable::map_t::const_iterator iter_end)
      : iter_(iter_begin), iter_end_(iter_end) {}
    void Init() override { first_flag_ = true; }
    const uint8_t* Next() override {
      if (iter_ == iter_end_)
        return nullptr;
      if (first_flag_) {
        first_flag_ = false;
      } else {
        ++iter_;
        if (iter_ == iter_end_) {
          return nullptr;
        }
      }
      return reinterpret_cast<const uint8_t*>(iter_->second.data());
    }

   private:
    bool first_flag_{true};
    MemoryTable::map_t::const_iterator iter_;
    MemoryTable::map_t::const_iterator iter_end_;
  };

  class ModifyHandle : public wing::ModifyHandle {
   public:
    ModifyHandle(MemoryTable& table) : table_(table) {}
    void Init() override {}
    bool Delete(std::string_view key) override { return table_.Delete(key); }
    bool Insert(std::string_view key, std::string_view tuple) override {
      return table_.Insert(key, tuple);
    }
    bool Update(std::string_view key, std::string_view tuple) override {
      return table_.Update(key, tuple);
    }

   private:
    MemoryTable& table_;
  };

  class SearchHandle : public wing::SearchHandle {
   public:
    SearchHandle(MemoryTable& table) : table_(table) {}
    void Init() override {}
    const uint8_t* Search(std::string_view key) override {
      return table_.Search(key);
    }

   private:
    MemoryTable& table_;
  };

  static MemoryTable New(TableSchema&& schema) {
    return MemoryTable(std::move(schema));
  }

  bool Insert(std::string_view key, std::string_view tuple) {
    auto it = index_.find(key);
    if (it != index_.end()) {
      return false;
    }
    ticks_ += 1;
    index_.insert({std::string(key), std::string(tuple)});
    return true;
  }

  bool Delete(std::string_view key) {
    auto it = index_.find(key);
    if (it == index_.end()) {
      return false;
    }
    index_.erase(it);
    return true;
  }

  const uint8_t* Search(std::string_view key) {
    auto it = index_.find(key);
    return it == index_.end()
               ? nullptr
               : reinterpret_cast<const uint8_t*>(it->second.data());
  }

  bool Update(std::string_view key, std::string_view tuple) {
    auto it = index_.find(key);
    if (it == index_.end()) {
      return false;
    }
    it->second = std::string(tuple);
    return true;
  }

  size_t TupleNum() { return index_.size(); }

  std::optional<std::string_view> GetMaxKey() {
    auto it = index_.rbegin();
    if (it == index_.rend()) {
      return std::nullopt;
    } else {
      return it->first;
    }
  }

  size_t GetTicks() { return ticks_; }

  const TableSchema& GetTableSchema() const { return schema_; }

  auto GetIndexBegin() { return index_.begin(); }

  auto GetIndexEnd() { return index_.end(); }

  auto GetIndexLower(std::string_view a) { return index_.lower_bound(a); }

  auto GetIndexUpper(std::string_view a) { return index_.upper_bound(a); }

 private:
  MemoryTable(TableSchema&& schema) : schema_(std::move(schema)) {}
  MemoryTable(map_t&& index, TableSchema&& schema, size_t ticks)
    : index_(std::move(index)), schema_(std::move(schema)), ticks_(ticks) {}
  map_t index_;
  TableSchema schema_;
  size_t ticks_{1};
  template <typename S>
  friend void tag_invoke(
      serde::tag_t<serde::serialize>, const MemoryTable& table, S s) {
    serde::serialize(table.ticks_, s);
    serde::serialize(table.schema_, s);
    serde::serialize(table.index_, s);
  }
  template <typename D>
  friend auto tag_invoke(
      serde::tag_t<serde::deserialize>, serde::type_tag_t<MemoryTable>, D d)
      -> Result<MemoryTable, typename D::Error> {
    size_t ticks =
        EXTRACT_RESULT(serde::deserialize(serde::type_tag<size_t>, d));
    TableSchema schema =
        EXTRACT_RESULT(serde::deserialize(serde::type_tag<TableSchema>, d));
    MemoryTable::map_t index = EXTRACT_RESULT(
        serde::deserialize(serde::type_tag<MemoryTable::map_t>, d));
    return MemoryTable(std::move(index), std::move(schema), ticks);
  }
};

class MemoryTableStorage {
 private:
  // https://stackoverflow.com/questions/20317413/what-are-transparent-comparators
  typedef std::map<std::string, MemoryTable, std::less<>> map_t;

 public:
  ~MemoryTableStorage() {
    std::ofstream out(path_, std::ios::binary);
    serde::bin_stream::Serializer s(out);
    serde::serialize(tables_, s);
  }
  static auto Open(std::filesystem::path&& path, bool create_if_missing,
      size_t max_buf_pages) -> Result<MemoryTableStorage, io::Error> {
    (void)max_buf_pages;
    if (!std::filesystem::exists(path)) {
      if (create_if_missing)
        return Create(std::move(path));
      else
        return io::Error::from(io::ErrorKind::NotFound);
    }
    std::ifstream in(path, std::ios::binary);
    serde::bin_stream::Deserializer d(in);
    map_t tables =
        EXTRACT_RESULT(serde::deserialize(serde::type_tag<map_t>, d));
    DBSchema schema;
    for (auto& kv : tables)
      schema.AddTable(kv.second.GetTableSchema());
    return MemoryTableStorage(
        std::move(tables), std::move(path), std::move(schema));
  }
  std::unique_ptr<Iterator<const uint8_t*>> GetIterator(
      std::string_view table_name) {
    auto& table = GetMemoryTable(table_name);
    return std::make_unique<MemoryTable::Iterator>(
        table.GetIndexBegin(), table.GetIndexEnd());
  }

  std::unique_ptr<Iterator<const uint8_t*>> GetRangeIterator(
      std::string_view table_name, std::tuple<std::string_view, bool, bool> L,
      std::tuple<std::string_view, bool, bool> R) {
    auto& table = GetMemoryTable(table_name);
    auto iter_l = std::get<1>(L)   ? table.GetIndexBegin()
                  : std::get<2>(L) ? table.GetIndexLower(std::get<0>(L))
                                   : table.GetIndexUpper(std::get<0>(L));
    auto iter_r = std::get<1>(R)   ? table.GetIndexEnd()
                  : std::get<2>(R) ? table.GetIndexUpper(std::get<0>(R))
                                   : table.GetIndexLower(std::get<0>(R));
    return std::make_unique<MemoryTable::Iterator>(iter_l, iter_r);
  }

  std::unique_ptr<ModifyHandle> GetModifyHandle(std::string_view table_name) {
    return std::make_unique<MemoryTable::ModifyHandle>(
        GetMemoryTable(table_name));
  }
  std::unique_ptr<SearchHandle> GetSearchHandle(std::string_view table_name) {
    return std::make_unique<MemoryTable::SearchHandle>(
        GetMemoryTable(table_name));
  }
  void Create(const TableSchema& schema) {
    auto table_name = schema.GetName();
    tables_.emplace(table_name, MemoryTable::New(TableSchema(schema)));
    schema_.AddTable(schema);
  }
  void Drop(std::string_view table_name) {
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
      tables_.erase(it);
      schema_.RemoveTable(table_name);
    }
  }
  size_t TupleNum(std::string_view table_name) {
    return GetMemoryTable(table_name).TupleNum();
  }

  std::optional<std::string_view> GetMaxKey(std::string_view table_name) {
    return GetMemoryTable(table_name).GetMaxKey();
  }

  size_t GetTicks(std::string_view table_name) {
    return GetMemoryTable(table_name).GetTicks();
  }

  const DBSchema& GetDBSchema() const { return schema_; }

 private:
  MemoryTableStorage(std::filesystem::path&& path) : path_(std::move(path)) {}
  MemoryTableStorage(
      map_t&& tables, std::filesystem::path&& path, DBSchema&& schema)
    : tables_(std::move(tables)),
      path_(std::move(path)),
      schema_(std::move(schema)) {}
  static MemoryTableStorage Create(std::filesystem::path path) {
    return MemoryTableStorage(std::move(path));
  }
  MemoryTable& GetMemoryTable(std::string_view table_name) {
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
      DB_ERR("no such table");
    } else {
      return it->second;
    }
  }
  map_t tables_;
  std::filesystem::path path_;
  DBSchema schema_;
};

}  // namespace wing

#endif  // SAKURA_ONLINE_JUDGE

#endif  // SAKURA_MEMORY_STORAGE_H__
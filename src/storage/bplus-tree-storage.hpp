#ifndef BPLUS_TREE_STORAGE_H_
#define BPLUS_TREE_STORAGE_H_

#include "bplus-tree.hpp"
#include "blob.hpp"
#include "common/logging.hpp"
#include "storage.hpp"
#include "catalog/schema.hpp"

#include <compare>

namespace wing {

class BPlusTreeTable {
 private:
  typedef BPlusTree<std::compare_three_way> tree_t;
 public:
  class Iterator : public wing::Iterator<const uint8_t *> {
   public:
    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;
    Iterator(Iterator&& iter)
      : first_flag_(iter.first_flag_), iter_(std::move(iter.iter_)) {}
    Iterator& operator=(Iterator&& iter) {
      first_flag_ = std::move(iter.first_flag_);
      iter_ = std::move(iter.iter_);
      return *this;
    }
    void Init() override { first_flag_ = true; }
    const uint8_t *Next() override {
      if (!first_flag_) {
        iter_.Next();
      } else {
        first_flag_ = false;
      }
      auto ret = iter_.Cur();
      if (!ret.has_value())
        return nullptr;
      std::string_view tuple = ret.value().second;
      return reinterpret_cast<const uint8_t *>(tuple.data());
    }
   private:
    Iterator(tree_t::Iter&& iter) : first_flag_(true), iter_(std::move(iter)) {}
    bool first_flag_;
    tree_t::Iter iter_;
    friend class BPlusTreeTable;
  };
  template <bool RIGHT_CLOSED>
  class RangeIterator : public wing::Iterator<const uint8_t *> {
   public:
    RangeIterator(tree_t::Iter&& iter, std::string&& end)
      : first_flag_(true), iter_(std::move(iter)), end_(std::move(end)) {}
    void Init() override { DB_ERR("Not supported yet!"); }
    const uint8_t *Next() override {
      if (!first_flag_) {
        iter_.Next();
      } else {
        first_flag_ = false;
      }
      auto ret = iter_.Cur();
      if (!ret.has_value())
        return nullptr;
      auto [key, tuple] = ret.value();
      if (RIGHT_CLOSED) {
        if (key > end_)
          return nullptr;
      } else {
        if (key >= end_)
          return nullptr;
      }
      return reinterpret_cast<const uint8_t *>(tuple.data());
    }
   private:
    bool first_flag_;
    tree_t::Iter iter_;
    std::string end_;
  };
  class ModifyHandle : public wing::ModifyHandle {
   public:
    void Init() override {}
    bool Delete(std::string_view key) override {
      return table_.Delete(key);
    }
    bool Insert(std::string_view key, std::string_view value) override {
      return table_.Insert(key, value);
    }
    bool Update(std::string_view key, std::string_view value) override {
      return table_.Update(key, value);
    }
   private:
    ModifyHandle(BPlusTreeTable& table) : table_(table) {}
    BPlusTreeTable& table_;
    friend class BPlusTreeTable;
  };
  class SearchHandle : public wing::SearchHandle {
   public:
    void Init() override {}
    const uint8_t *Search(std::string_view key) override {
      auto ret = tree_.Get(key);
      if (!ret.has_value())
        return nullptr;
      last_ = std::move(ret.value());
      return reinterpret_cast<const uint8_t *>(last_.data());
    }
   private:
    SearchHandle(tree_t& tree) : tree_(tree) {}
    tree_t& tree_;
    std::string last_;
    friend class BPlusTreeTable;
  };

  BPlusTreeTable(const BPlusTreeTable&) = delete;
  BPlusTreeTable& operator=(const BPlusTreeTable&) = delete;
  BPlusTreeTable(BPlusTreeTable&& table)
    : schema_(std::move(table.schema_)), tree_(std::move(table.tree_)) {}
  BPlusTreeTable& operator=(BPlusTreeTable&& rhs) {
    schema_ = std::move(rhs.schema_);
    tree_ = std::move(rhs.tree_);
    return *this;
  }
  void Drop() {
    tree_.Destroy();
  }
  Iterator Begin() {
    return Iterator(tree_.Begin());
  }
  auto GetRangeIterator(std::pair<std::string_view, bool> L,
    std::pair<std::string_view, bool> R
  ) -> std::unique_ptr<wing::Iterator<const uint8_t *>> {
    tree_t::Iter iter = L.second
      ? tree_.LowerBound(L.first)
      : tree_.UpperBound(L.first);
    if (R.second) {
      return std::make_unique<RangeIterator<false>>(
        RangeIterator<false>(std::move(iter), std::string(R.first)));
    } else {
      return std::make_unique<RangeIterator<true>>(
        RangeIterator<true>(std::move(iter), std::string(R.first)));
    }
  }

  bool Delete(std::string_view key) {
    return tree_.Delete(key);
  }
  bool Insert(std::string_view key, std::string_view value) {
    bool succeed = tree_.Insert(key, value);
    if (succeed)
      ticks_ += 1;
    return succeed;
  }
  bool Update(std::string_view key, std::string_view value) {
    auto exists = tree_.Update(key, value);
    return exists;
  }
  ModifyHandle GetModifyHandle() {
    return ModifyHandle(*this);
  }
  SearchHandle GetSearchHandle() {
    return SearchHandle(tree_);
  }
  
  size_t TupleNum() {
    return tree_.TupleNum();
  }
  std::optional<std::string_view> GetMaxKey() {
    return tree_.MaxKey();
  }
  size_t GetTicks() { return ticks_; }
  const TableSchema& GetTableSchema() { return schema_; }
 private:
  BPlusTreeTable(TableSchema&& schema, tree_t&& tree)
    : schema_(std::move(schema)), tree_(std::move(tree)) {}
  TableSchema schema_;
  tree_t tree_;
  size_t ticks_;
  friend class BPlusTreeStorage;
};

struct TableMetaPages {
  static TableMetaPages from_bytes(std::string_view bytes) {
    assert(bytes.size() == sizeof(TableMetaPages));
    return *reinterpret_cast<const TableMetaPages *>(bytes.data());
  }
  // Meta page ID of the b+ tree for data.
  pgid_t data;
  // Head page ID of the blob for schema.
  pgid_t schema;
};

class BPlusTreeStorage {
 public:
  static auto Open(
    std::filesystem::path&& path, bool create_if_missing, size_t max_buf_pages
  ) -> Result<BPlusTreeStorage, io::Error> {
    if (!std::filesystem::exists(path)) {
      if (create_if_missing)
        return Create(std::move(path), max_buf_pages);
    }
    auto pgm = EXTRACT_RESULT(PageManager::Open(path, max_buf_pages));
    pgid_t meta;
    pgm->GetPlainPage(pgm->SuperPageID()).Read(&meta, 0, sizeof(meta));
    auto map = tree_t::Open(*pgm, meta);
    DBSchema db_schema;
    auto it = map.Begin();
    for (;;) {
      auto ret = it.Cur();
      if (!ret.has_value())
        break;
      std::string_view table_name = ret.value().first;
      auto meta = TableMetaPages::from_bytes(ret.value().second);
      auto schema_err = serde::bin_stream::from_string<TableSchema>(
        Blob::Open(*pgm, meta.schema).Read());
      if (schema_err.index() == 1)
        DB_ERR("Corrupted schema of table {}", table_name);
      TableSchema schema = std::move(std::get<0>(schema_err));
      db_schema.AddTable(schema);
      it.Next();
    }
    return BPlusTreeStorage(std::move(pgm), std::move(map),
      std::move(db_schema));
  }
  auto GetIterator(std::string_view table_name
  ) -> std::unique_ptr<Iterator<const uint8_t*>> {
    return std::make_unique<BPlusTreeTable::Iterator>(
      GetTable(table_name).Begin());
  }
  auto GetRangeIterator(std::string_view table_name,
    std::pair<std::string_view, bool> L, std::pair<std::string_view, bool> R
  ) -> std::unique_ptr<Iterator<const uint8_t*>> {
    return GetTable(table_name).GetRangeIterator(L, R);
  }

  std::unique_ptr<ModifyHandle> GetModifyHandle(std::string_view table_name) {
    return std::make_unique<BPlusTreeTable::ModifyHandle>(
      GetTable(table_name).GetModifyHandle());
  }
  std::unique_ptr<SearchHandle> GetSearchHandle(std::string_view table_name) {
    return std::make_unique<BPlusTreeTable::SearchHandle>(
      GetTable(table_name).GetSearchHandle());
  }
  std::optional<io::Error> Create(const TableSchema& schema) {
    auto table_name = schema.GetName();
    tree_t tree = tree_t::Create(*pgm_);
    auto blob = Blob::Create(*pgm_);
    blob.Rewrite(serde::bin_stream::to_string(schema));

    TableMetaPages meta{
      .data = tree.MetaPageID(),
      .schema = blob.MetaPageID(),
    };
    bool succeed = map_table_name_to_meta_pages_.Insert(table_name,
      std::string_view(reinterpret_cast<const char *>(&meta), sizeof(meta)));
    if (!succeed) {
      tree.Destroy();
      blob.Destroy();
      return io::Error::from(io::ErrorKind::AlreadyExists);
    }
    auto ret = cached_tables_.emplace(
      std::string(table_name),
      BPlusTreeTable(TableSchema(schema), std::move(tree)));
    if (!ret.second)
      DB_ERR("{}", table_name);
    schema_.AddTable(schema);
    return std::nullopt;
  }
  std::optional<io::Error> Drop(std::string_view table_name) {
    auto ret = map_table_name_to_meta_pages_.Take(table_name);
    if (!ret.has_value())
      return io::Error::from(io::ErrorKind::NotFound);
    auto meta = TableMetaPages::from_bytes(ret.value());
    auto it = cached_tables_.find(std::string(table_name));
    if (it != cached_tables_.end()) {
      assert(it->second.tree_.MetaPageID() == meta.data);
      it->second.Drop();
      cached_tables_.erase(it);
    } else {
      tree_t::Open(*pgm_, meta.data).Destroy();
    }
    Blob::Open(*pgm_, meta.schema).Destroy();
    schema_.RemoveTable(table_name);
    return std::nullopt;
  }
  size_t TupleNum(std::string_view table_name) {
    return GetTable(table_name).TupleNum();
  }
  std::optional<std::string_view> GetMaxKey(std::string_view table_name) {
    return GetTable(table_name).GetMaxKey();
  }
  size_t GetTicks(std::string_view table_name) {
    return GetTable(table_name).GetTicks();
  }
  const DBSchema& GetDBSchema() const { return schema_; }
 private:
  typedef BPlusTree<std::compare_three_way> tree_t;
  BPlusTreeStorage(std::unique_ptr<PageManager> pgm, tree_t&& map,
      DBSchema&& db_schema)
    : pgm_(std::move(pgm)),
      map_table_name_to_meta_pages_(std::move(map)),
      schema_(std::move(db_schema)) {}
  static auto Create(
    std::filesystem::path path,
    size_t max_buf_pages
  ) -> BPlusTreeStorage {
    auto pgm = PageManager::Create(path, max_buf_pages);
    auto map = tree_t::Create(*pgm);
    pgid_t meta = map.MetaPageID();
    pgm->GetPlainPage(pgm->SuperPageID())
      .Write(0,
        std::string_view(reinterpret_cast<const char *>(&meta), sizeof(meta)));
    return BPlusTreeStorage(std::move(pgm), std::move(map), DBSchema{});
  }
  BPlusTreeTable& GetTable(std::string_view table_name) {
    auto it_find = cached_tables_.find(std::string(table_name));
    if (it_find != cached_tables_.end())
      return it_find->second;
    auto ret = map_table_name_to_meta_pages_.Get(table_name);
    if (!ret.has_value())
      DB_ERR("no such table");
    auto meta = TableMetaPages::from_bytes(ret.value());
    auto schema_err = serde::bin_stream::from_string<TableSchema>(
      Blob::Open(*pgm_, meta.schema).Read());
    if (schema_err.index() == 1)
      DB_ERR("Corrupted schema of table {}", table_name);
    TableSchema schema = std::move(std::get<0>(schema_err));
    BPlusTreeTable table(std::move(schema), tree_t::Open(*pgm_, meta.data));
    auto [it, succeed] =
      cached_tables_.emplace(std::string(table_name), std::move(table));
    if (!succeed)
      DB_ERR("Concurrency issue?");
    return it->second;
  }
  std::unique_ptr<PageManager> pgm_;
  // Table name -> TableMetaPages
  tree_t map_table_name_to_meta_pages_;
  std::unordered_map<std::string, BPlusTreeTable> cached_tables_;
  DBSchema schema_;
};

}

#endif // BPLUS_TREE_STORAGE_H_

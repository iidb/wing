#ifndef BPLUS_TREE_STORAGE_H_
#define BPLUS_TREE_STORAGE_H_

#include <compare>

#include "blob.hpp"
#include "bplus-tree.hpp"
#include "catalog/schema.hpp"
#include "common/logging.hpp"
#include "storage.hpp"

namespace wing {

/* The Base class of all B+trees containing only deconstructor.*/
class AbstractBPlusTreeTable {
 public:
  virtual ~AbstractBPlusTreeTable() = default;
};

/* Compare function for string (CHAR or VARCHAR). */
using StringKeyCompare = std::compare_three_way;

/* Compare function for integer key (INT32 or INT64). */
struct IntegerKeyCompare {
  std::weak_ordering operator()(std::string_view L, std::string_view R) const {
    // Compare integers.
    // Integers in storage may be 4 bytes, but queried with 8 bytes.
    int64_t l = L.size() == 4 ? *reinterpret_cast<const int32_t*>(L.data())
                              : *reinterpret_cast<const int64_t*>(L.data());
    int64_t r = R.size() == 4 ? *reinterpret_cast<const int32_t*>(R.data())
                              : *reinterpret_cast<const int64_t*>(R.data());
    return l <=> r;
  }
};

/* Compare function for integer key (FLOAT64). */
struct FloatKeyCompare {
  std::weak_ordering operator()(std::string_view L, std::string_view R) const {
    double l = *reinterpret_cast<const double*>(L.data());
    double r = *reinterpret_cast<const double*>(R.data());
    // Float comparation is std::partial_ordering.
    // But we don't need to consider invalid cases.
    if (l < r)
      return std::weak_ordering::less;
    else if (l > r)
      return std::weak_ordering::greater;
    else
      return std::weak_ordering::equivalent;
  }
};

template <typename KeyCompare>
class BPlusTreeTable : public AbstractBPlusTreeTable {
 private:
  using tree_t = BPlusTree<KeyCompare>;

 public:
  class Iterator : public wing::Iterator<const uint8_t*> {
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
    Iterator(typename tree_t::Iter&& iter)
      : first_flag_(true), iter_(std::move(iter)) {}
    void Init() override { first_flag_ = true; }
    const uint8_t* Next() override {
      if (!first_flag_) {
        iter_.Next();
      } else {
        first_flag_ = false;
      }
      auto ret = iter_.Cur();
      if (!ret.has_value())
        return nullptr;
      std::string_view tuple = ret.value().second;
      return reinterpret_cast<const uint8_t*>(tuple.data());
    }

   private:
    bool first_flag_;
    typename tree_t::Iter iter_;
    friend class BPlusTreeTable<KeyCompare>;
  };
  template <bool RIGHT_CLOSED, bool RIGHT_NOLIMIT>
  class RangeIterator : public wing::Iterator<const uint8_t*> {
   public:
    RangeIterator(typename tree_t::Iter&& iter, std::string&& end)
      : first_flag_(true), iter_(std::move(iter)), end_(std::move(end)) {}
    /* TODO: implement the real Init(). */
    void Init() override { first_flag_ = true; }
    const uint8_t* Next() override {
      if (!first_flag_) {
        iter_.Next();
      } else {
        first_flag_ = false;
      }
      auto ret = iter_.Cur();
      if (!ret.has_value())
        return nullptr;
      auto [key, tuple] = ret.value();
      if (!RIGHT_NOLIMIT) {
        if (RIGHT_CLOSED) {
          if (KeyCompare()(key, end_) > 0)
            return nullptr;
        } else {
          if (KeyCompare()(key, end_) >= 0)
            return nullptr;
        }
      }
      return reinterpret_cast<const uint8_t*>(tuple.data());
    }

   private:
    bool first_flag_;
    typename tree_t::Iter iter_;
    std::string end_;
  };
  class ModifyHandle : public wing::ModifyHandle {
   public:
    ModifyHandle(BPlusTreeTable& table) : table_(table) {}
    void Init() override {}
    bool Delete(std::string_view key) override { return table_.Delete(key); }
    bool Insert(std::string_view key, std::string_view value) override {
      return table_.Insert(key, value);
    }
    bool Update(std::string_view key, std::string_view value) override {
      return table_.Update(key, value);
    }

   private:
    BPlusTreeTable& table_;
    friend class BPlusTreeTable<KeyCompare>;
  };
  class SearchHandle : public wing::SearchHandle {
   public:
    SearchHandle(tree_t& tree) : tree_(tree) {}
    void Init() override {}
    const uint8_t* Search(std::string_view key) override {
      auto ret = tree_.Get(key);
      if (!ret.has_value())
        return nullptr;
      last_ = std::move(ret.value());
      return reinterpret_cast<const uint8_t*>(last_.data());
    }

   private:
    tree_t& tree_;
    std::string last_;
    friend class BPlusTreeTable<KeyCompare>;
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
  void Drop() { tree_.Destroy(); }
  Iterator Begin() { return Iterator(tree_.Begin()); }
  std::unique_ptr<wing::Iterator<const uint8_t*>> GetIterator() {
    return std::make_unique<Iterator>(tree_.Begin());
  }
  auto GetRangeIterator(std::tuple<std::string_view, bool, bool> L,
      std::tuple<std::string_view, bool, bool> R)
      -> std::unique_ptr<wing::Iterator<const uint8_t*>> {
    auto iter = std::get<1>(L)   ? tree_.Begin()
                : std::get<2>(L) ? tree_.LowerBound(std::get<0>(L))
                                 : tree_.UpperBound(std::get<0>(L));
    if (std::get<1>(R)) {
      // right is empty. i.e. not limited.
      return std::make_unique<RangeIterator<false, true>>(
          std::move(iter), std::string(std::get<0>(R)));
    } else if (std::get<2>(R)) {
      // right closed.
      return std::make_unique<RangeIterator<true, false>>(
          std::move(iter), std::string(std::get<0>(R)));
    } else {
      // right open.
      return std::make_unique<RangeIterator<false, false>>(
          std::move(iter), std::string(std::get<0>(R)));
    }
  }

  bool Delete(std::string_view key) { return tree_.Delete(key); }
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
  std::unique_ptr<wing::ModifyHandle> GetModifyHandle() {
    return std::make_unique<ModifyHandle>(*this);
  }
  std::unique_ptr<wing::SearchHandle> GetSearchHandle() {
    return std::make_unique<SearchHandle>(tree_);
  }
  size_t TupleNum() { return tree_.TupleNum(); }
  std::optional<std::string_view> GetMaxKey() { return tree_.MaxKey(); }
  size_t GetTicks() { return ticks_; }
  const TableSchema& GetTableSchema() { return schema_; }
  BPlusTreeTable(TableSchema&& schema, tree_t&& tree)
    : schema_(std::move(schema)), tree_(std::move(tree)) {}

 private:
  TableSchema schema_;
  tree_t tree_;
  size_t ticks_;
  friend class BPlusTreeStorage;
};

struct TableMetaPages {
  static TableMetaPages from_bytes(std::string_view bytes) {
    assert(bytes.size() == sizeof(TableMetaPages));
    return *reinterpret_cast<const TableMetaPages*>(bytes.data());
  }
  // Meta page ID of the b+ tree for data.
  pgid_t data;
  // Head page ID of the blob for schema.
  pgid_t schema;
};

class BPlusTreeStorage {
 public:
  static auto Open(std::filesystem::path&& path, bool create_if_missing,
      size_t max_buf_pages) -> Result<BPlusTreeStorage, io::Error> {
    if (!std::filesystem::exists(path)) {
      if (create_if_missing)
        return Create(std::move(path), max_buf_pages);
    }
    auto pgm = EXTRACT_RESULT(PageManager::Open(path, max_buf_pages));
    pgid_t meta;
    pgm->GetPlainPage(pgm->SuperPageID()).Read(&meta, 0, sizeof(meta));
    // Table B+Tree use StringKeyCompare by default.
    auto map = BPlusTree<StringKeyCompare>::Open(*pgm, meta);
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
    return BPlusTreeStorage(
        std::move(pgm), std::move(map), std::move(db_schema));
  }
  auto GetIterator(std::string_view table_name)
      -> std::unique_ptr<Iterator<const uint8_t*>> {
    return ApplyFuncOnTable<std::unique_ptr<Iterator<const uint8_t*>>>(
        GetPKType(table_name), GetTable(table_name),
        [](auto a) { return a->GetIterator(); });
  }

  auto GetRangeIterator(std::string_view table_name,
      std::tuple<std::string_view, bool, bool> L,
      std::tuple<std::string_view, bool, bool> R)
      -> std::unique_ptr<Iterator<const uint8_t*>> {
    return ApplyFuncOnTable<std::unique_ptr<Iterator<const uint8_t*>>>(
        GetPKType(table_name), GetTable(table_name),
        [&L, &R](auto a) { return a->GetRangeIterator(L, R); });
  }

  std::unique_ptr<wing::ModifyHandle> GetModifyHandle(
      std::string_view table_name) {
    return ApplyFuncOnTable<std::unique_ptr<wing::ModifyHandle>>(
        GetPKType(table_name), GetTable(table_name),
        [](auto a) { return a->GetModifyHandle(); });
  }
  std::unique_ptr<wing::SearchHandle> GetSearchHandle(
      std::string_view table_name) {
    return ApplyFuncOnTable<std::unique_ptr<wing::SearchHandle>>(
        GetPKType(table_name), GetTable(table_name),
        [](auto a) { return a->GetSearchHandle(); });
  }
  std::optional<io::Error> Create(const TableSchema& schema) {
    auto table_name = schema.GetName();
    auto blob = Blob::Create(*pgm_);
    schema_.AddTable(schema);
    blob.Rewrite(serde::bin_stream::to_string(schema));

    auto create_func = [&](auto&& tree) -> std::optional<io::Error> {
      TableMetaPages meta{
          .data = tree.MetaPageID(),
          .schema = blob.MetaPageID(),
      };
      bool succeed = map_table_name_to_meta_pages_.Insert(table_name,
          std::string_view(reinterpret_cast<const char*>(&meta), sizeof(meta)));
      if (!succeed) {
        tree.Destroy();
        blob.Destroy();
        return io::Error::from(io::ErrorKind::AlreadyExists);
      }
      auto ret = cached_tables_.emplace(std::string(table_name),
          CreateBPlusTreeTable(TableSchema(schema), std::move(tree)));
      if (!ret.second)
        DB_ERR("{}", table_name);
      return std::nullopt;
    };
    // Primary key type
    auto pk_type = schema.GetPrimaryKeySchema().type_;
    if (pk_type == FieldType::INT32 || pk_type == FieldType::INT64) {
      auto tree = BPlusTree<IntegerKeyCompare>::Create(*pgm_);
      return create_func(std::move(tree));
    } else if (pk_type == FieldType::CHAR || pk_type == FieldType::VARCHAR) {
      auto tree = BPlusTree<StringKeyCompare>::Create(*pgm_);
      return create_func(std::move(tree));
    } else if (pk_type == FieldType::FLOAT64) {
      auto tree = BPlusTree<FloatKeyCompare>::Create(*pgm_);
      return create_func(std::move(tree));
    } else {
      DB_ERR("Invalid primary key type.");
    }
  }
  std::optional<io::Error> Drop(std::string_view table_name) {
    auto ret = map_table_name_to_meta_pages_.Take(table_name);
    if (!ret.has_value())
      return io::Error::from(io::ErrorKind::NotFound);
    auto meta = TableMetaPages::from_bytes(ret.value());
    auto it = cached_tables_.find(std::string(table_name));
    if (it != cached_tables_.end()) {
      ApplyFuncOnTable<void>(
          GetPKType(table_name), it->second.get(), [&meta](auto a) {
            assert(a->tree_.MetaPageID() == meta.data);
            a->Drop();
          });
      cached_tables_.erase(it);
    } else {
      // Table B+Tree
      BPlusTree<StringKeyCompare>::Open(*pgm_, meta.data).Destroy();
    }
    Blob::Open(*pgm_, meta.schema).Destroy();
    schema_.RemoveTable(table_name);
    return std::nullopt;
  }
  size_t TupleNum(std::string_view table_name) {
    return ApplyFuncOnTable<size_t>(GetPKType(table_name), GetTable(table_name),
        [](auto a) { return a->TupleNum(); });
  }
  std::optional<std::string_view> GetMaxKey(std::string_view table_name) {
    return ApplyFuncOnTable<std::optional<std::string_view>>(
        GetPKType(table_name), GetTable(table_name),
        [](auto a) { return a->GetMaxKey(); });
  }
  size_t GetTicks(std::string_view table_name) {
    return ApplyFuncOnTable<size_t>(GetPKType(table_name), GetTable(table_name),
        [](auto a) { return a->GetTicks(); });
  }
  const DBSchema& GetDBSchema() const { return schema_; }

 private:
  BPlusTreeStorage(std::unique_ptr<PageManager> pgm,
      BPlusTree<StringKeyCompare>&& map, DBSchema&& db_schema)
    : pgm_(std::move(pgm)),
      map_table_name_to_meta_pages_(std::move(map)),
      schema_(std::move(db_schema)) {}
  static auto Create(std::filesystem::path path, size_t max_buf_pages)
      -> BPlusTreeStorage {
    auto pgm = PageManager::Create(path, max_buf_pages);
    auto map = BPlusTree<StringKeyCompare>::Create(*pgm);
    pgid_t meta = map.MetaPageID();
    pgm->GetPlainPage(pgm->SuperPageID())
        .Write(0, std::string_view(
                      reinterpret_cast<const char*>(&meta), sizeof(meta)));
    return BPlusTreeStorage(std::move(pgm), std::move(map), DBSchema{});
  }
  AbstractBPlusTreeTable* GetTable(std::string_view table_name) {
    auto it_find = cached_tables_.find(std::string(table_name));
    if (it_find != cached_tables_.end())
      return it_find->second.get();
    auto ret = map_table_name_to_meta_pages_.Get(table_name);
    if (!ret)
      DB_ERR("no such table");

    auto meta = TableMetaPages::from_bytes(ret.value());
    auto schema_err = serde::bin_stream::from_string<TableSchema>(
        Blob::Open(*pgm_, meta.schema).Read());
    if (schema_err.index() == 1)
      DB_ERR("Corrupted schema of table {}", table_name);

    TableSchema schema = std::move(std::get<0>(schema_err));
    // Primary key type
    auto pk_type = schema.GetPrimaryKeySchema().type_;
    // For each primary key type, use the corresponding Open() function.
    if (pk_type == FieldType::INT32 || pk_type == FieldType::INT64) {
      auto [it, succeed] = cached_tables_.emplace(std::string(table_name),
          std::make_unique<BPlusTreeTable<IntegerKeyCompare>>(std::move(schema),
              BPlusTree<IntegerKeyCompare>::Open(*pgm_, meta.data)));
      if (!succeed)
        DB_ERR("Concurrency issue?");
      return it->second.get();
    } else if (pk_type == FieldType::CHAR || pk_type == FieldType::VARCHAR) {
      auto [it, succeed] = cached_tables_.emplace(std::string(table_name),
          std::make_unique<BPlusTreeTable<StringKeyCompare>>(std::move(schema),
              BPlusTree<StringKeyCompare>::Open(*pgm_, meta.data)));
      if (!succeed)
        DB_ERR("Concurrency issue?");
      return it->second.get();
    } else if (pk_type == FieldType::FLOAT64) {
      auto [it, succeed] = cached_tables_.emplace(std::string(table_name),
          std::make_unique<BPlusTreeTable<FloatKeyCompare>>(std::move(schema),
              BPlusTree<FloatKeyCompare>::Open(*pgm_, meta.data)));
      if (!succeed)
        DB_ERR("Concurrency issue?");
      return it->second.get();
    } else {
      DB_ERR("Invalid primary key type.");
    }
  }
  /**
   *  Choose correct primary key field type for each B+tree.
   *  type is the pk type.
   *  tree is the pointer.
   *  func is the function applied to the B+tree.
   * */
  template <typename RetType, typename F>
  RetType ApplyFuncOnTable(
      FieldType type, AbstractBPlusTreeTable* tree, F&& func) {
    if (type == FieldType::INT32 || type == FieldType::INT64) {
      auto t_tree = static_cast<BPlusTreeTable<IntegerKeyCompare>*>(tree);
      return func(t_tree);
    } else if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
      auto t_tree = static_cast<BPlusTreeTable<StringKeyCompare>*>(tree);
      return func(t_tree);
    } else if (type == FieldType::FLOAT64) {
      auto t_tree = static_cast<BPlusTreeTable<FloatKeyCompare>*>(tree);
      return func(t_tree);
    } else {
      DB_ERR("Invalid btree primary key type.");
    }
  }
  /* Get primary key type by table name. */
  FieldType GetPKType(std::string_view table_name) const {
    auto index = schema_.Find(table_name);
    if (!index) {
      DB_ERR("Invalid table name.");
    }
    return schema_.GetTables()[index.value()].GetPrimaryKeySchema().type_;
  }
  template <typename T>
  std::unique_ptr<AbstractBPlusTreeTable> CreateBPlusTreeTable(
      TableSchema&& schema, BPlusTree<T>&& tree) const {
    return std::make_unique<BPlusTreeTable<T>>(
        std::move(schema), std::move(tree));
  }

  std::unique_ptr<PageManager> pgm_;
  // Table name -> TableMetaPages
  BPlusTree<StringKeyCompare> map_table_name_to_meta_pages_;
  std::unordered_map<std::string, std::unique_ptr<AbstractBPlusTreeTable>>
      cached_tables_;
  DBSchema schema_;
};

}  // namespace wing

#endif  // BPLUS_TREE_STORAGE_H_

#pragma once

#include "storage/lsm/lsm.hpp"
#include "storage/storage.hpp"

namespace wing {

class LSMStorage : public Storage {
 public:
  static std::unique_ptr<Storage> Open(std::filesystem::path&& path,
      bool create_if_missing, const lsm::Options& options) {
    if (!std::filesystem::exists(path)) {
      if (create_if_missing) {
        std::filesystem::create_directories(path / "tables");
        return std::unique_ptr<Storage>(new LSMStorage(path, options));
      } else {
        throw DBException("Cannot find database under {}", path.string());
      }
    }
    std::ifstream in(path.string() + "/metadata", std::ios::binary);
    serde::bin_stream::Deserializer d(in);
    auto db_schema_result = serde::deserialize(serde::type_tag<DBSchema>, d);
    if (db_schema_result.index() == 1) {
      throw DBException("DB Schema in LSM storage is invalid.");
    }
    auto db = new LSMStorage(path, options);
    db->schema_ = std::get<0>(db_schema_result);
    for (uint32_t i = 0; i < db->schema_.GetTables().size(); i++) {
      auto name = db->schema_.GetTables()[i].GetName();
      lsm::Options options0 = options;
      options0.create_new = false;
      options0.db_path = fmt::format("{}/tables/t'{}'", path.string(), name);
      auto lsm = std::make_unique<lsm::DBImpl>(options0);
      if (!lsm) {
        throw DBException("LSM tree of `{}' is invalid.", name);
      }
      auto table = std::make_unique<Table>();
      table->lsm_ = std::move(lsm);
      auto tick_result = serde::deserialize(serde::type_tag<uint64_t>, d);
      if (tick_result.index() == 1) {
        throw DBException("tick in LSM storage is invalid.");
      }
      table->tick_ = std::get<0>(tick_result);
      db->tables_.emplace(std::string(name), std::move(table));
    }
    return std::unique_ptr<Storage>(db);
  }

  void Save() {
    std::ofstream out(db_path_ + "/metadata", std::ios::binary);
    serde::bin_stream::Serializer s(out);
    serde::serialize(schema_, s);
    for (auto& [_, table] : tables_) {
      serde::serialize(uint64_t(table->tick_), s);
      table->lsm_->Save();
    }
  }

  ~LSMStorage() { Save(); }

  class Table {
   public:
    std::unique_ptr<lsm::DBImpl> lsm_;
    size_t tick_{0};
  };

  class LSMModifyHandle : public ModifyHandle {
   public:
    LSMModifyHandle(Table& table) : table_(table) {}

    void Init() override {}
    bool Delete(std::string_view key) override {
      table_.lsm_->Del(key);
      return true;
    }
    bool Insert(std::string_view key, std::string_view value) override {
      std::string v0;
      if (table_.lsm_->Get(key, &v0)) {
        return false;
      }
      table_.lsm_->Put(key, value);
      table_.tick_ += 1;
      return true;
    }
    bool Update(std::string_view key, std::string_view new_value) override {
      table_.lsm_->Put(key, new_value);
      return true;
    }

   private:
    Table& table_;
  };

  class LSMSearchHandle : public SearchHandle {
   public:
    LSMSearchHandle(lsm::DBImpl* lsm) : lsm_(lsm) {}

    void Init() override {}
    const uint8_t* Search(std::string_view key) override {
      if (!lsm_->Get(key, &value_)) {
        return nullptr;
      }
      return reinterpret_cast<const uint8_t*>(value_.data());
    }

   private:
    lsm::DBImpl* lsm_;
    std::string value_;
  };

  class LSMIterator : public wing::Iterator<const uint8_t*> {
   public:
    LSMIterator(lsm::DBImpl* lsm, std::tuple<std::string_view, bool, bool> L,
        std::tuple<std::string_view, bool, bool> R)
      : it_(std::get<1>(L) ? lsm->Begin() : lsm->Seek(std::get<0>(L))) {
      if (!std::get<1>(L) && !std::get<2>(L) && it_.Valid() &&
          it_.key() == std::get<0>(L)) {
        it_.Next();
      }
      first_flag_ = true;
      R_ = R;
    }
    void Init() override {}
    const uint8_t* Next() override {
      if (first_flag_) {
        first_flag_ = false;
      } else {
        if (it_.Valid())
          it_.Next();
      }
      if (!it_.Valid() ||
          (!std::get<1>(R_) &&
              (std::get<2>(R_) ? it_.key() > std::get<0>(R_)
                               : it_.key() >= std::get<0>(R_)))) {
        return nullptr;
      }
      return reinterpret_cast<const uint8_t*>(it_.value().data());
    }

   private:
    bool first_flag_{true};
    lsm::DBIterator it_;
    std::tuple<std::string, bool, bool> R_;
  };

  void Create(const TableSchema& schema) override {
    auto table_name = schema.GetName();
    lsm::Options option = options_;
    option.create_new = true;
    option.db_path = fmt::format("{}/tables/t'{}'", db_path_, table_name);
    std::filesystem::create_directory(option.db_path);
    auto table = std::make_unique<Table>();
    table->lsm_ = std::make_unique<lsm::DBImpl>(option);
    table->tick_ = 0;
    tables_.emplace(table_name, std::move(table));
    schema_.AddTable(schema);
  }

  void Drop(std::string_view table_name) override {
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
      tables_.erase(it);
      std::string table_path =
          fmt::format("{}/tables/t'{}'/", db_path_, table_name);
      std::filesystem::remove_all(table_path);
      schema_.RemoveTable(table_name);
    }
  }

  size_t GetTicks(std::string_view table_name) override {
    return GetTable(table_name).tick_;
  }

  const DBSchema& GetDBSchema() const override { return schema_; }

  std::unique_ptr<Iterator<const uint8_t*>> GetIterator(
      std::string_view table_name) override {
    return std::make_unique<LSMIterator>(GetTable(table_name).lsm_.get(),
        std::make_tuple("", true, false), std::make_tuple("", true, false));
  }

  std::unique_ptr<Iterator<const uint8_t*>> GetRangeIterator(
      std::string_view table_name, std::tuple<std::string_view, bool, bool> L,
      std::tuple<std::string_view, bool, bool> R) {
    return std::make_unique<LSMIterator>(GetTable(table_name).lsm_.get(), L, R);
  }

  std::unique_ptr<ModifyHandle> GetModifyHandle(
      std::unique_ptr<TxnExecCtx> ctx) override {
    return std::make_unique<LSMModifyHandle>(GetTable(ctx->table_name_));
  }

  std::unique_ptr<SearchHandle> GetSearchHandle(
      std::unique_ptr<TxnExecCtx> ctx) override {
    return std::make_unique<LSMSearchHandle>(
        GetTable(ctx->table_name_).lsm_.get());
  }

 private:
  LSMStorage(const std::filesystem::path& path, const lsm::Options& options) {
    db_path_ = path.string();
    options_ = options;
  }
  Table& GetTable(std::string_view table_name) {
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
      DB_ERR("no such table");
    } else {
      return *(it->second);
    }
  }

  std::string db_path_;
  std::map<std::string, std::unique_ptr<Table>, std::less<>> tables_;
  lsm::Options options_;
  DBSchema schema_;
};

}  // namespace wing

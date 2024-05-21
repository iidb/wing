#include "catalog/db.hpp"

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "common/logging.hpp"
#include "storage/bplus_tree/bplus-tree-storage.hpp"
#include "storage/lsm/lsm_storage.hpp"
#include "storage/memory_storage.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/lock_mode.hpp"
#include "transaction/txn_manager.hpp"

namespace wing {

class DB::Impl {
 private:
 public:
  static auto Open(std::filesystem::path path, WingOptions& options)
      -> std::unique_ptr<DB::Impl> {
    std::unique_ptr<Storage> table_storage;
    if (options.storage_backend_name == "memory") {
      table_storage =
          MemoryTableStorage::Open(std::move(path), options.create_if_missing);
    } else if (options.storage_backend_name == "b+tree") {
      table_storage = BPlusTreeStorage::Open(std::move(path),
          options.create_if_missing, options.buf_pool_max_page);
    } else if (options.storage_backend_name == "lsm") {
      table_storage = LSMStorage::Open(
          std::move(path), options.create_if_missing, options.lsm_options);
    } else {
      DB_ERR("This is not valid Storage backend name! `{}'",
          options.storage_backend_name);
    }

    return std::unique_ptr<DB::Impl>(
        new DB::Impl(std::move(table_storage), options));
  }

  void CreateTable(txn_id_t txn_id, const TableSchema& schema) {
    // It is safe to directly acquire X lock since it is the highest level.
    txn_manager_.GetLockManager().AcquireTableLock(

        schema.GetName(), LockMode::X, TxnManager::GetTxn(txn_id).value());
    table_storage_->Create(schema);
    tick_table_[std::string(schema.GetName())] = 1;
  }

  void DropTable(txn_id_t txn_id, std::string_view table_name) {
    // It is safe to directly acquire X lock since it is the highest level.
    txn_manager_.GetLockManager().AcquireTableLock(
        table_name, LockMode::X, TxnManager::GetTxn(txn_id).value());
    table_storage_->Drop(table_name);
    tick_table_.erase(tick_table_.find(table_name));
  }

  std::unique_ptr<Iterator<const uint8_t*>> GetIterator(
      txn_id_t txn_id, std::string_view table_name) {
    // P4 TODO
    return table_storage_->GetIterator(table_name);
  }

  // For simplicity, range iterator holds the S lock on the whole table.
  std::unique_ptr<Iterator<const uint8_t*>> GetRangeIterator(txn_id_t txn_id,
      std::string_view table_name, std::tuple<std::string_view, bool, bool> L,
      std::tuple<std::string_view, bool, bool> R) {
    // P4 TODO
    return table_storage_->GetRangeIterator(table_name, L, R);
  }

  std::unique_ptr<ModifyHandle> GetModifyHandle(
      txn_id_t txn_id, std::string_view table_name) {
    // P4 TODO
    return table_storage_->GetModifyHandle(std::make_unique<TxnExecCtx>(
        txn_id, std::string(table_name), &txn_manager_.GetLockManager()));
  }

  std::unique_ptr<SearchHandle> GetSearchHandle(
      txn_id_t txn_id, std::string_view table_name) {
    // P4 TODO
    return table_storage_->GetSearchHandle(std::make_unique<TxnExecCtx>(
        txn_id, std::string(table_name), &txn_manager_.GetLockManager()));
  }

  // No need to handle txn logic.
  GenPKHandle GetGenPKHandle(txn_id_t txn_id, std::string_view table_name) {
    (void)txn_id;
    return GenPKHandle(&tick_table_.find(table_name)->second);
  }

  const DBSchema& GetDBSchema() const { return table_storage_->GetDBSchema(); }

  TxnManager& GetTxnManager() { return txn_manager_; }

  void UpdateStats(std::string_view table_name, TableStatistics&& stat) {
    table_stats_[std::string(table_name)] =
        std::make_unique<TableStatistics>(std::move(stat));
  }

  // Return the pointer to the statistic data. Return null if there is no stats.
  const TableStatistics* GetTableStat(std::string_view table_name) const {
    auto it = table_stats_.find(table_name);
    if (it == table_stats_.end())
      return nullptr;
    return it->second.get();
  }

  const WingOptions& GetOptions() const { return options_; }

 private:
  Impl(std::unique_ptr<Storage> table_storage, WingOptions& options)
    : table_storage_(std::move(table_storage)),
      txn_manager_(*table_storage_),
      options_(options) {
    for (auto& a : table_storage_->GetDBSchema().GetTables()) {
      std::string name(a.GetName());
      size_t tick = table_storage_->GetTicks(a.GetName());
      tick_table_[name].store(tick, std::memory_order_relaxed);
    }
  }
  WingOptions& options_;
  std::unique_ptr<Storage> table_storage_;
  std::map<std::string, std::unique_ptr<TableStatistics>, std::less<>>
      table_stats_;

  std::map<std::string, std::atomic<int64_t>, std::less<>> tick_table_;

  // global txn manager and lock manager (inside txn_manager_).
  TxnManager txn_manager_;
};

DB::DB(std::string_view file_name, WingOptions& options) {
  std::filesystem::path path(file_name);
  ptr_ = DB::Impl::Open(path, options);
}

DB::~DB() {}

void DB::CreateTable(txn_id_t txn_id, const TableSchema& schema) {
  return ptr_->CreateTable(txn_id, schema);
}

void DB::DropTable(txn_id_t txn_id, std::string_view table_name) {
  return ptr_->DropTable(txn_id, table_name);
}

std::unique_ptr<Iterator<const uint8_t*>> DB::GetIterator(
    txn_id_t txn_id, std::string_view table_name) {
  return ptr_->GetIterator(txn_id, table_name);
}

std::unique_ptr<Iterator<const uint8_t*>> DB::GetRangeIterator(txn_id_t txn_id,
    std::string_view table_name, std::tuple<std::string_view, bool, bool> L,
    std::tuple<std::string_view, bool, bool> R) {
  return ptr_->GetRangeIterator(txn_id, table_name, L, R);
}

std::unique_ptr<ModifyHandle> DB::GetModifyHandle(
    txn_id_t txn_id, std::string_view table_name) {
  return ptr_->GetModifyHandle(txn_id, table_name);
}

GenPKHandle DB::GetGenPKHandle(txn_id_t txn_id, std::string_view table_name) {
  return ptr_->GetGenPKHandle(txn_id, table_name);
}

const DBSchema& DB::GetDBSchema() const { return ptr_->GetDBSchema(); }

std::unique_ptr<SearchHandle> DB::GetSearchHandle(
    txn_id_t txn_id, std::string_view table_name) {
  return ptr_->GetSearchHandle(txn_id, table_name);
}

void DB::UpdateStats(std::string_view table_name, TableStatistics&& stat) {
  ptr_->UpdateStats(table_name, std::move(stat));
}

const TableStatistics* DB::GetTableStat(std::string_view table_name) const {
  return ptr_->GetTableStat(table_name);
}

TxnManager& DB::GetTxnManager() { return ptr_->GetTxnManager(); }

const WingOptions& DB::GetOptions() const { return ptr_->GetOptions(); }

}  // namespace wing

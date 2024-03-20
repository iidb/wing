#include "catalog/db.hpp"

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "common/logging.hpp"
#include "storage/bplus_tree/bplus-tree-storage.hpp"
#include "storage/memory_storage.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/lock_mode.hpp"
#include "transaction/txn_manager.hpp"

namespace wing {

class DB::Impl {
 private:
  // using StorageBackend = BPlusTreeStorage;
  using StorageBackend = MemoryTableStorage;

 public:
  static auto Open(std::filesystem::path path, bool create_if_missing,
      size_t max_buf_pages) -> Result<std::unique_ptr<DB::Impl>, io::Error> {
    auto table_storage = EXTRACT_RESULT(StorageBackend::Open(
        std::move(path), create_if_missing, max_buf_pages));
    return std::unique_ptr<DB::Impl>(new DB::Impl(std::move(table_storage)));
  }

  void CreateTable(txn_id_t txn_id, const TableSchema& schema) {
    // It is safe to directly acquire X lock since it is the highest level.
    txn_manager_.GetLockManager().AcquireTableLock(

        schema.GetName(), LockMode::X, TxnManager::GetTxn(txn_id).value());
    table_storage_.Create(schema);
    tick_table_[std::string(schema.GetName())] = 1;
  }

  void DropTable(txn_id_t txn_id, std::string_view table_name) {
    // It is safe to directly acquire X lock since it is the highest level.
    txn_manager_.GetLockManager().AcquireTableLock(
        table_name, LockMode::X, TxnManager::GetTxn(txn_id).value());
    table_storage_.Drop(table_name);
    tick_table_.erase(tick_table_.find(table_name));
  }

  std::unique_ptr<Iterator<const uint8_t*>> GetIterator(
      txn_id_t txn_id, std::string_view table_name) {
    // P4 TODO
    return table_storage_.GetIterator(table_name);
  }

  // For simplicity, range iterator holds the S lock on the whole table.
  std::unique_ptr<Iterator<const uint8_t*>> GetRangeIterator(txn_id_t txn_id,
      std::string_view table_name, std::tuple<std::string_view, bool, bool> L,
      std::tuple<std::string_view, bool, bool> R) {
    // P4 TODO
    return table_storage_.GetRangeIterator(table_name, L, R);
  }

  std::unique_ptr<ModifyHandle> GetModifyHandle(
      txn_id_t txn_id, std::string_view table_name) {
    // P4 TODO
    return table_storage_.GetModifyHandle(std::make_unique<TxnExecCtx>(
        txn_id, std::string(table_name), &txn_manager_.GetLockManager()));
  }

  std::unique_ptr<SearchHandle> GetSearchHandle(
      txn_id_t txn_id, std::string_view table_name) {
    // P4 TODO
    return table_storage_.GetSearchHandle(std::make_unique<TxnExecCtx>(
        txn_id, std::string(table_name), &txn_manager_.GetLockManager()));
  }

  // No need to handle txn logic.
  GenPKHandle GetGenPKHandle(txn_id_t txn_id, std::string_view table_name) {
    (void)txn_id;
    return GenPKHandle(&tick_table_.find(table_name)->second);
  }

  const DBSchema& GetDBSchema() const { return table_storage_.GetDBSchema(); }

  size_t GetPrimaryKey(std::string_view table_name) {
    auto ret = table_storage_.GetMaxKey(table_name);
    if (!ret.has_value()) {
      return 0;
    }
    std::string_view max_key = ret.value();
    if (max_key.size() != sizeof(size_t)) {
      DB_ERR("Currently only support primary keys of type size_t");
    }
    return *reinterpret_cast<const size_t*>(max_key.data()) + 1;
  }

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

 private:
  Impl(StorageBackend&& table_storage)
    : table_storage_(std::move(table_storage)), txn_manager_(table_storage_) {
    for (auto& a : table_storage_.GetDBSchema().GetTables()) {
      std::string name(a.GetName());
      size_t tick = table_storage_.GetTicks(a.GetName());
      tick_table_[name].store(tick, std::memory_order_relaxed);
    }
  }
  StorageBackend table_storage_;
  std::map<std::string, std::unique_ptr<TableStatistics>, std::less<>>
      table_stats_;

  std::map<std::string, std::atomic<int64_t>, std::less<>> tick_table_;

  // global txn manager and lock manager (inside txn_manager_).
  TxnManager txn_manager_;
};

DB::DB(std::string_view file_name) {
  std::filesystem::path path(file_name);
  // 128MB of buffer
  auto ret = DB::Impl::Open(path, true, 32 * 1024);
  if (ret.index() == 1)
    throw std::get<1>(ret).to_string();
  ptr_ = std::move(std::get<0>(ret));
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

}  // namespace wing

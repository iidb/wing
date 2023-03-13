#include "catalog/db.hpp"

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "common/logging.hpp"
#include "storage/bplus-tree-storage.hpp"
#include "storage/memory_storage.hpp"

namespace wing {

class DB::Impl {
 private:
  // using StorageBackend = BPlusTreeStorage;
  using StorageBackend = MemoryTableStorage;

 public:
  static auto Open(
    std::filesystem::path path, bool create_if_missing, size_t max_buf_pages
  ) -> Result<std::unique_ptr<DB::Impl>, io::Error> {
    auto table_storage = EXTRACT_RESULT(StorageBackend::Open(
      std::move(path), create_if_missing, max_buf_pages));
    return std::unique_ptr<DB::Impl>(new DB::Impl(std::move(table_storage)));
  }

  void CreateTable(const TableSchema& schema) {
    table_storage_.Create(schema);
    tick_table_[std::string(schema.GetName())] = 1;
  }

  void DropTable(std::string_view table_name) {
    table_storage_.Drop(table_name);
    tick_table_.erase(tick_table_.find(table_name));
  }

  std::unique_ptr<Iterator<const uint8_t*>> GetIterator(size_t txn_id, std::string_view table_name) {
    (void)txn_id;
    return table_storage_.GetIterator(table_name);
  }

  std::unique_ptr<Iterator<const uint8_t*>> GetRangeIterator(size_t txn_id, std::string_view table_name, std::pair<std::string_view, bool> L,
                                                             std::pair<std::string_view, bool> R) {
    (void)txn_id;
    return table_storage_.GetRangeIterator(table_name, L, R);
  }

  std::unique_ptr<ModifyHandle> GetModifyHandle(size_t txn_id, std::string_view table_name) {
    (void)txn_id;
    return table_storage_.GetModifyHandle(table_name);
  }

  std::unique_ptr<SearchHandle> GetSearchHandle(size_t txn_id, std::string_view table_name) {
    (void)txn_id;
    return table_storage_.GetSearchHandle(table_name);
  }

  GenPKHandle GetGenPKHandle(size_t txn_id, std::string_view table_name) {
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

  void Rollback(size_t txn_id) {}

  void Commit(size_t txn_id) {}

  size_t GenerateTxnID() {
    auto ret = ++txn_id_;
    metadata_mu_.lock_shared();
    std::unique_lock lck(txn_metadata_lock_table_mu_);
    txn_metadata_lock_table_[ret] = 0;
    return ret;
  }

  void AcquireMetadataWLock(size_t txn_id) {
    metadata_mu_.unlock_shared();
    metadata_mu_.lock();
    std::unique_lock lck(txn_metadata_lock_table_mu_);
    txn_metadata_lock_table_[txn_id] = 1;
  }

  void ReleaseMetadataLock(size_t txn_id) {
    size_t flag;
    {
      std::unique_lock lck(txn_metadata_lock_table_mu_);
      flag = txn_metadata_lock_table_[txn_id];
      txn_metadata_lock_table_.erase(txn_id);
    }

    if (flag == 0) {
      metadata_mu_.unlock_shared();
    } else {
      metadata_mu_.unlock();
    }
  }

  void UpdateStats(std::string_view table_name, TableStatistics&& stat) {
    table_stats_[std::string(table_name)] = std::make_unique<TableStatistics>(std::move(stat));
  }

  // Return the pointer to the statistic data. Return null if there is no stats.
  const TableStatistics* GetTableStat(std::string_view table_name) const {
    auto it = table_stats_.find(table_name);
    if (it == table_stats_.end()) return nullptr;
    return it->second.get();
  }

 private:
  Impl(StorageBackend&& table_storage) : table_storage_(std::move(table_storage)) {
    for (auto& a : table_storage_.GetDBSchema().GetTables()) {
      std::string name(a.GetName());
      size_t tick = table_storage_.GetTicks(a.GetName());
      tick_table_[name].store(tick, std::memory_order_relaxed);
    }
  }
  StorageBackend table_storage_;
  std::map<std::string, std::unique_ptr<TableStatistics>, std::less<>> table_stats_;

  // txn_id 0 is for default transaction in shell.
  std::atomic<size_t> txn_id_{1};

  std::shared_mutex metadata_mu_;
  std::map<size_t, size_t> txn_metadata_lock_table_;
  std::mutex txn_metadata_lock_table_mu_;

  std::map<std::string, std::atomic<int64_t>, std::less<>> tick_table_;
};

DB::DB(std::string_view file_name) {
  std::filesystem::path path(file_name);
  // 128MB of buffer
  auto ret = DB::Impl::Open(path, true, 32 * 1024);
  if (ret.index() == 1) throw std::get<1>(ret).to_string();
  ptr_ = std::move(std::get<0>(ret));
}

DB::~DB() {}

void DB::CreateTable(const TableSchema& schema) { return ptr_->CreateTable(schema); }

void DB::DropTable(std::string_view table_name) { return ptr_->DropTable(table_name); }

std::unique_ptr<Iterator<const uint8_t*>> DB::GetIterator(size_t txn_id, std::string_view table_name) {
  return ptr_->GetIterator(txn_id, table_name);
}

std::unique_ptr<Iterator<const uint8_t*>> DB::GetRangeIterator(size_t txn_id, std::string_view table_name, std::pair<std::string_view, bool> L,
                                                               std::pair<std::string_view, bool> R) {
  return ptr_->GetRangeIterator(txn_id, table_name, L, R);
}

std::unique_ptr<ModifyHandle> DB::GetModifyHandle(size_t txn_id, std::string_view table_name) { return ptr_->GetModifyHandle(txn_id, table_name); }

GenPKHandle DB::GetGenPKHandle(size_t txn_id, std::string_view table_name) { return ptr_->GetGenPKHandle(txn_id, table_name); }

const DBSchema& DB::GetDBSchema() const { return ptr_->GetDBSchema(); }

void DB::Rollback(size_t txn_id) {}

void DB::Commit(size_t txn_id) {}

size_t DB::GenerateTxnID() { return ptr_->GenerateTxnID(); }

void DB::AcquireMetadataWLock(size_t txn_id) { ptr_->AcquireMetadataWLock(txn_id); }

void DB::ReleaseMetadataLock(size_t txn_id) { ptr_->ReleaseMetadataLock(txn_id); }

std::unique_ptr<SearchHandle> DB::GetSearchHandle(size_t txn_id, std::string_view table_name) { return ptr_->GetSearchHandle(txn_id, table_name); }

void DB::UpdateStats(std::string_view table_name, TableStatistics&& stat) { ptr_->UpdateStats(table_name, std::move(stat)); }

const TableStatistics* DB::GetTableStat(std::string_view table_name) const { return ptr_->GetTableStat(table_name); }

}  // namespace wing

#ifndef SAKURA_TXN_MANAGER_H__
#define SAKURA_TXN_MANAGER_H__

#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "lock_manager.hpp"
#include "txn.hpp"

namespace wing {
class BPlusTreeStorage;
// Unique txn manager per instance.
class TxnManager {
 public:
  // txn_table holds the memory ownership of all txns.
  static std::unordered_map<txn_id_t, std::unique_ptr<Txn>> txn_table_;
  // latch to protect concurrent access to txn_table.
  static std::shared_mutex rw_latch_;

  TxnManager(BPlusTreeStorage& storage, LockManager::DL_Algorithm dl_algorithm =
                                            LockManager::DL_Algorithm::WAIT_DIE)
    : lock_manager_(dl_algorithm), storage_(storage) {}
  TxnManager(const TxnManager&) = delete;
  TxnManager& operator=(const TxnManager&) = delete;

  Txn* Begin();

  void Commit(Txn* txn);

  void Abort(Txn* txn);

  LockManager& GetLockManager() { return lock_manager_; }

  static std::optional<Txn*> GetTxn(txn_id_t txn_id) {
    std::shared_lock lock(rw_latch_);
    if (txn_table_.find(txn_id) == txn_table_.end()) {
      return std::nullopt;
    }
    return txn_table_[txn_id].get();
  }

 private:
  // unique txn_id assigned to each txn. Monotonically increasing.
  // txn_id_t::max is a placeholder for invalid txn id. See txn.hpp.
  std::atomic<txn_id_t> txn_id_{0};
  // Unique lock manager per instance.
  LockManager lock_manager_;
  // The storage of the db instance. Used for rollback during abort.
  BPlusTreeStorage& storage_;

  void ReleaseAllLocks(Txn* txn);
};
}  // namespace wing
#endif
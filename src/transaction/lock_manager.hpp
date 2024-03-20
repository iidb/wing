#pragma once

#include <condition_variable>
#include <list>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/exception.hpp"
#include "lock_mode.hpp"
#include "txn.hpp"

namespace wing {
class LockManager;

// This is used for SearchHandle and ModifyHandle to pass the context so that it
// can lock corresponding tuple.
struct TxnExecCtx {
  TxnExecCtx(
      txn_id_t txn_id, std::string &&table_name, LockManager *lock_manager)
    : txn_id_(txn_id),
      table_name_(std::move(table_name)),
      lock_manager_(lock_manager) {}
  txn_id_t txn_id_;
  std::string table_name_;
  LockManager *lock_manager_;
};

class LockManager {
 public:
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode mode)
      : txn_id_(txn_id), mode_(mode), granted_(false) {}
    ~LockRequest() = default;

    txn_id_t txn_id_;
    LockMode mode_;
    bool granted_;
  };

  class LockRequestList {
   public:
    LockRequestList() = default;
    LockRequestList(const LockRequestList &) = delete;
    LockRequestList &operator=(const LockRequestList &) = delete;

    std::list<std::shared_ptr<LockRequest>> list_;
    std::mutex latch_;
    std::condition_variable cv_;
    txn_id_t upgrading_ = INVALID_TXN_ID;
  };

  enum class DL_Algorithm {
    NONE,
    WAIT_DIE,    // default
    WOUND_WAIT,  // bonus
    DL_DETECT,   // maybe next year
  };

  LockManager(DL_Algorithm dl_algorithm = DL_Algorithm::WAIT_DIE)
    : dl_algorithm_(dl_algorithm) {}
  LockManager(const LockManager &) = delete;
  LockManager &operator=(const LockManager &) = delete;

  void AcquireTableLock(std::string_view table_name, LockMode mode, Txn *txn);
  void ReleaseTableLock(std::string_view table_name, LockMode mode, Txn *txn);
  void AcquireTupleLock(std::string_view table_name, std::string_view key,
      LockMode mode, Txn *txn);
  void ReleaseTupleLock(std::string_view table_name, std::string_view key,
      LockMode mode, Txn *txn);

 private:
  // table-level lock table
  std::unordered_map<std::string /*table_name*/,
      std::unique_ptr<LockRequestList>>
      table_lock_table_;
  std::mutex table_lock_table_latch_;

  // row-level lock table
  std::unordered_map<std::string /*table_name*/,
      std::unordered_map<std::string /*key*/, std::unique_ptr<LockRequestList>>>
      tuple_lock_table_;
  std::mutex tuple_lock_table_latch_;

  // Deadlock handling Algorithm. Default: WAIT_DIE
  DL_Algorithm dl_algorithm_;
};
}  // namespace wing

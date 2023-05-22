#ifndef SAKURA_TXN_H__
#define SAKURA_TXN_H__

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <shared_mutex>
#include <stack>
#include <unordered_map>
#include <unordered_set>

#include "lock_mode.hpp"

namespace wing {
enum ModifyType {
  INSERT,
  DELETE,
  UPDATE,
};
struct ModifyRecord {
  ModifyRecord(ModifyType type, std::string_view table_name,
      std::string_view key, std::optional<std::string_view> old_value)
    : type_(type), table_name_(table_name), key_(key), old_value_(old_value) {}
  ModifyType type_;
  std::string table_name_;
  std::string key_;
  std::optional<std::string> old_value_;  // leave as empty if type is INSERT
};

enum class TxnState { GROWING, SHRINKING, COMMITTED, ABORTED };

typedef size_t txn_id_t;
static constexpr txn_id_t INVALID_TXN_ID = std::numeric_limits<txn_id_t>::max();

// Txn represents a transaction abstraction which includes the state and locks
// it currently holds.
class Txn {
 public:
  Txn(txn_id_t txn_id) : state_(TxnState::GROWING), txn_id_(txn_id) {}

  Txn(const Txn &) = delete;
  Txn &operator=(const Txn &) = delete;

  std::atomic<TxnState> state_;
  txn_id_t txn_id_;

  // Each txn keeps the set of locks it has **acquired**. Not include the locks
  // that are waiting in the queue.
  std::unordered_map<LockMode, std::unordered_set<std::string /*table_name*/>>
      table_lock_set_;

  std::unordered_map<LockMode, std::unordered_map<std::string /*table_name*/,
                                   std::unordered_set<std::string /*key*/>>>
      tuple_lock_set_;

  // latch to protect concurrent access to txn's data structures.
  std::shared_mutex rw_latch_;

  // Modify records for rollback operations.
  // We will not test rollback for tables for now.
  // TODO (in future semester): change to WAL.
  std::stack<ModifyRecord> modify_records_;
};
}  // namespace wing
#endif
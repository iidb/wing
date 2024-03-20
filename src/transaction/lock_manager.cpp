#include "lock_manager.hpp"

#include <memory>
#include <shared_mutex>

#include "common/exception.hpp"
#include "common/logging.hpp"
#include "fmt/core.h"
#include "transaction/txn.hpp"
#include "transaction/txn_manager.hpp"

namespace wing {
void LockManager::AcquireTableLock(
    std::string_view table_name, LockMode mode, Txn *txn) {
  // P4 TODO
}

void LockManager::ReleaseTableLock(
    std::string_view table_name, LockMode mode, Txn *txn) {
  // P4 TODO
}

void LockManager::AcquireTupleLock(std::string_view table_name,
    std::string_view key, LockMode mode, Txn *txn) {
  // P4 TODO
}

void LockManager::ReleaseTupleLock(std::string_view table_name,
    std::string_view key, LockMode mode, Txn *txn) {
  // P4 TODO
}
}  // namespace wing

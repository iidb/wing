#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "common/error.hpp"
#include "common/exception.hpp"
#include "common/stopwatch.hpp"
#include "fmt/core.h"
#include "instance/instance.hpp"
#include "storage/bplus-tree-storage.hpp"
#include "test.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/lock_mode.hpp"
#include "transaction/txn.hpp"
#include "transaction/txn_manager.hpp"
#include "zipf.hpp"

using namespace wing;
using namespace wing::wing_testing;
using fmt::format;

void ExpectTableLockSetSize(
    Txn *txn, size_t s, size_t x, size_t is, size_t ix, size_t six) {
  std::shared_lock l(txn->rw_latch_);
  EXPECT_EQ(txn->table_lock_set_[LockMode::S].size(), s);
  EXPECT_EQ(txn->table_lock_set_[LockMode::X].size(), x);
  EXPECT_EQ(txn->table_lock_set_[LockMode::IS].size(), is);
  EXPECT_EQ(txn->table_lock_set_[LockMode::IX].size(), ix);
  EXPECT_EQ(txn->table_lock_set_[LockMode::SIX].size(), six);
}

void ExpectTupleLockSetSize(
    Txn *txn, std::string_view table_name, size_t s, size_t x) {
  std::shared_lock l(txn->rw_latch_);
  EXPECT_EQ(txn->tuple_lock_set_[LockMode::S][table_name.data()].size(), s);
  EXPECT_EQ(txn->tuple_lock_set_[LockMode::X][table_name.data()].size(), x);
}

auto SetUpDummyStorage() -> BPlusTreeStorage {
  std::filesystem::remove("__tmp_dummy");
  std::filesystem::path path("__tmp_dummy");
  auto ret = BPlusTreeStorage::Open(std::move(path), true, 32 * 1024);
  if (ret.index() == 1)
    throw std::get<1>(ret).to_string();
  return std::move(std::get<0>(ret));
}

// Execute the func as a transaction until the transaction is committed.
// If "die" in wait-die occurs or multiple upgrade happens, abort and re-execute
// transaction.
// CAUTION: it requries all db->Execute in func passing in txn_id.
template <typename F>
void ExecAsATxnUntilCommit(Instance *db, TxnManager &txn_manager, F &&func) {
  bool committed = false;
  while (!committed) {
    auto txn = txn_manager.Begin();
    try {
      func(db, txn->txn_id_);
      txn_manager.Commit(txn);
      committed = true;
    } catch (TxnDLAbortException &e) {
      txn_manager.Abort(txn);
      std::this_thread::sleep_for(
          std::chrono::milliseconds(5 + std::rand() % 10));
    } catch (MultiUpgradeException &e) {
      txn_manager.Abort(txn);
      std::this_thread::sleep_for(
          std::chrono::milliseconds(5 + std::rand() % 10));
    }
  }
}

template <bool pk_varchar = true>
void ExecUpdate(Instance *db, std::string_view table, std::string_view pk_name,
    std::string_view pk, std::string_view values, txn_id_t txn_id) {
  if constexpr (pk_varchar) {
    EXPECT_TRUE(
        db->Execute(format("delete from {} where {}='{}';", table, pk_name, pk),
              txn_id)
            .Valid());
    EXPECT_TRUE(db->Execute(format("insert into {} values ('{}',{});", table,
                                pk, values),
                      txn_id)
                    .Valid());
  } else {
    EXPECT_TRUE(
        db->Execute(
              format("delete from {} where {}={};", table, pk_name, pk), txn_id)
            .Valid());
    EXPECT_TRUE(
        db->Execute(format("insert into {} values ({},{});", table, pk, values),
              txn_id)
            .Valid());
  }
}

TEST(LockManagerTest, InvalidBehaviorTest1) {
  auto dummy_storage = SetUpDummyStorage();
  auto txn_manager = TxnManager(dummy_storage);
  auto &lock_manager = txn_manager.GetLockManager();

  Txn *txn1 = txn_manager.Begin();
  txn_manager.Abort(txn1);
  EXPECT_THROW(lock_manager.AcquireTableLock("t", LockMode::S, txn1),
      TxnInvalidBehaviorException);

  Txn *txn2 = txn_manager.Begin();
  lock_manager.AcquireTableLock("t", LockMode::S, txn2);
  ExpectTableLockSetSize(txn2, 1, 0, 0, 0, 0);
  lock_manager.ReleaseTableLock("t", LockMode::S, txn2);
  ExpectTableLockSetSize(txn2, 0, 0, 0, 0, 0);
  EXPECT_THROW(lock_manager.AcquireTableLock("t2", LockMode::S, txn2),
      TxnInvalidBehaviorException);

  Txn *txn4 = txn_manager.Begin();
  lock_manager.AcquireTableLock("t", LockMode::IS, txn4);
  lock_manager.AcquireTupleLock("t", "key", LockMode::S, txn4);
  ExpectTupleLockSetSize(txn4, "t", 1, 0);
  lock_manager.ReleaseTupleLock("t", "key", LockMode::S, txn4);
  ExpectTupleLockSetSize(txn4, "t", 0, 0);
  EXPECT_THROW(lock_manager.AcquireTableLock("t2", LockMode::IS, txn4),
      TxnInvalidBehaviorException);
}

TEST(LockManagerTest, InvalidBehaviorTest2) {
  auto dummy_storage = SetUpDummyStorage();
  auto txn_manager = TxnManager(dummy_storage);
  auto &lock_manager = txn_manager.GetLockManager();

  // Incompatible upgrade
  for (auto mode : {LockMode::IS, LockMode::IX}) {
    Txn *txn1 = txn_manager.Begin();
    lock_manager.AcquireTableLock("t", LockMode::S, txn1);
    EXPECT_THROW(lock_manager.AcquireTableLock("t", mode, txn1),
        TxnInvalidBehaviorException);
    txn_manager.Abort(txn1);
  }

  for (auto mode : {LockMode::IS, LockMode::S}) {
    Txn *txn1 = txn_manager.Begin();
    lock_manager.AcquireTableLock("t", LockMode::IX, txn1);
    EXPECT_THROW(lock_manager.AcquireTableLock("t", mode, txn1),
        TxnInvalidBehaviorException);
    txn_manager.Abort(txn1);
  }

  for (auto mode : {LockMode::S, LockMode::IS, LockMode::IX}) {
    Txn *txn1 = txn_manager.Begin();
    lock_manager.AcquireTableLock("t", LockMode::SIX, txn1);
    EXPECT_THROW(lock_manager.AcquireTableLock("t", mode, txn1),
        TxnInvalidBehaviorException);
    txn_manager.Abort(txn1);
  }

  for (auto mode : {LockMode::S, LockMode::IS, LockMode::IX, LockMode::SIX}) {
    Txn *txn1 = txn_manager.Begin();
    lock_manager.AcquireTableLock("t", LockMode::X, txn1);
    EXPECT_THROW(lock_manager.AcquireTableLock("t", mode, txn1),
        TxnInvalidBehaviorException);
    txn_manager.Abort(txn1);
  }
}

TEST(LockManagerTest, InvalidBehaviorTest3) {
  auto dummy_storage = SetUpDummyStorage();
  auto txn_manager = TxnManager(dummy_storage);
  auto &lock_manager = txn_manager.GetLockManager();

  // Tuple lock should ensure correctness of table lock.
  Txn *txn1 = txn_manager.Begin();
  EXPECT_THROW(lock_manager.AcquireTupleLock("t", "key", LockMode::X, txn1),
      TxnInvalidBehaviorException);
  txn1 = txn_manager.Begin();
  lock_manager.AcquireTableLock("t", LockMode::IS, txn1);
  EXPECT_THROW(lock_manager.AcquireTupleLock("t", "key", LockMode::X, txn1),
      TxnInvalidBehaviorException);
  // Tuple lock should not get intention lock.
  txn1 = txn_manager.Begin();
  EXPECT_THROW(lock_manager.AcquireTupleLock("t", "key", LockMode::IX, txn1),
      TxnInvalidBehaviorException);
  txn1 = txn_manager.Begin();
  EXPECT_THROW(lock_manager.AcquireTupleLock("t", "key", LockMode::IS, txn1),
      TxnInvalidBehaviorException);
  txn_manager.Abort(txn1);
}

TEST(LockManagerTest, MultiUpgradeTest) {
  auto dummy_storage = SetUpDummyStorage();
  auto txn_manager = TxnManager(dummy_storage);
  auto &lock_manager = txn_manager.GetLockManager();

  // Multi upgrade
  Txn *txn1 = txn_manager.Begin();
  Txn *txn2 = txn_manager.Begin();
  Txn *txn3 = txn_manager.Begin();
  lock_manager.AcquireTableLock("t", LockMode::S, txn3);
  lock_manager.AcquireTableLock("t", LockMode::S, txn2);
  lock_manager.AcquireTableLock("t", LockMode::S, txn1);
  std::thread t([&lock_manager, txn1]() {
    lock_manager.AcquireTableLock("t", LockMode::X, txn1);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_THROW(lock_manager.AcquireTableLock("t", LockMode::X, txn2),
      MultiUpgradeException);
  txn_manager.Abort(txn2);
  lock_manager.ReleaseTableLock("t", LockMode::S, txn3);
  t.join();
  ExpectTableLockSetSize(txn1, 0, 1, 0, 0, 0);
  txn_manager.Commit(txn1);
  ExpectTableLockSetSize(txn1, 0, 0, 0, 0, 0);
}

// We do not test this for now. May be future semester.
TEST(LockManagerTest, DISABLED_MultiUpgradeTestBonus) {
  auto dummy_storage = SetUpDummyStorage();
  auto txn_manager = TxnManager(dummy_storage);
  auto &lock_manager = txn_manager.GetLockManager();

  // Multi upgrade
  Txn *txn1 = txn_manager.Begin();
  Txn *txn2 = txn_manager.Begin();
  Txn *txn3 = txn_manager.Begin();
  lock_manager.AcquireTableLock("t", LockMode::S, txn3);
  lock_manager.AcquireTableLock("t", LockMode::S, txn2);
  lock_manager.AcquireTableLock("t", LockMode::S, txn1);
  std::vector<std::thread> threads;
  threads.push_back(std::thread([&lock_manager, txn2]() {
    lock_manager.AcquireTableLock("t", LockMode::X, txn2);
  }));
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  threads.push_back(std::thread([&lock_manager, txn1]() {
    lock_manager.AcquireTableLock("t", LockMode::X, txn1);
  }));
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  lock_manager.ReleaseTableLock("t", LockMode::X, txn3);
  ExpectTableLockSetSize(txn2, 0, 1, 0, 0, 0);
  lock_manager.ReleaseTableLock("t", LockMode::X, txn2);
  ExpectTableLockSetSize(txn1, 0, 1, 0, 0, 0);
  txn_manager.Commit(txn1);
  ExpectTableLockSetSize(txn1, 0, 0, 0, 0, 0);
  for (auto &t : threads) {
    t.join();
  }
}

TEST(LockManagerTest, TableLockTest1) {
  auto dummy_storage = SetUpDummyStorage();
  auto txn_manager = TxnManager(dummy_storage);
  auto &lock_manager = txn_manager.GetLockManager();
  int txn_num = 5;
  std::vector<Txn *> txns;
  std::vector<std::string> table_names;
  for (int i = 0; i < txn_num; i++) {
    txns.push_back(txn_manager.Begin());
    table_names.push_back("table" + std::to_string(i));
  }
  // create 5 threads, each acquires shared locks on the 5 tables
  std::vector<std::thread> threads;
  for (int i = 0; i < txn_num; i++) {
    threads.push_back(
        std::thread([&txn_manager, &lock_manager, &txns, &table_names, i]() {
          for (auto &table_name : table_names) {
            lock_manager.AcquireTableLock(table_name, LockMode::S, txns[i]);
            EXPECT_EQ(txns[i]->state_, TxnState::GROWING);
          }
          for (auto &table_name : table_names) {
            lock_manager.ReleaseTableLock(table_name, LockMode::S, txns[i]);
            EXPECT_EQ(txns[i]->state_, TxnState::SHRINKING);
          }
          txn_manager.Commit(txns[i]);
          EXPECT_EQ(txns[i]->state_, TxnState::COMMITTED);
        }));
  }
  for (auto &thread : threads) {
    thread.join();
  }
}

// First S then upgrade to X
TEST(LockManagerTest, TableLockTest2) {
  auto dummy_storage = SetUpDummyStorage();
  auto txn_manager = TxnManager(dummy_storage);
  auto &lock_manager = txn_manager.GetLockManager();
  Txn *txn0 = txn_manager.Begin();
  std::string table_name = "table0";
  lock_manager.AcquireTableLock(table_name, LockMode::S, txn0);
  EXPECT_EQ(txn0->state_, TxnState::GROWING);
  ExpectTableLockSetSize(txn0, 1, 0, 0, 0, 0);
  lock_manager.AcquireTableLock(table_name, LockMode::X, txn0);
  ExpectTableLockSetSize(txn0, 0, 1, 0, 0, 0);
  txn_manager.Commit(txn0);
  EXPECT_EQ(txn0->state_, TxnState::COMMITTED);
  ExpectTableLockSetSize(txn0, 0, 0, 0, 0, 0);
}

// Three txns run concurrently. One txn upgrades to X and waiting.
TEST(LockManagerTest, TableLockOrderTest) {
  auto dummy_storage = SetUpDummyStorage();
  auto txn_manager = TxnManager(dummy_storage);
  auto &lock_manager = txn_manager.GetLockManager();
  int txn_num = 3;
  std::vector<Txn *> txns;
  std::string table_name = "table0";
  for (int i = 0; i < txn_num; i++) {
    txns.push_back(txn_manager.Begin());
  }
  std::vector<std::thread> threads;
  threads.push_back(std::thread([&]() {
    lock_manager.AcquireTableLock(table_name, LockMode::S, txns[0]);
    ExpectTableLockSetSize(txns[0], 1, 0, 0, 0, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    // it blocks until t2 and t3 release their locks.
    lock_manager.AcquireTableLock(table_name, LockMode::X, txns[0]);
    ExpectTableLockSetSize(txns[0], 0, 1, 0, 0, 0);
    ExpectTableLockSetSize(txns[1], 0, 0, 0, 0, 0);
    ExpectTableLockSetSize(txns[2], 0, 0, 0, 0, 0);
    lock_manager.ReleaseTableLock(table_name, LockMode::X, txns[0]);
    txn_manager.Commit(txns[0]);
    ExpectTableLockSetSize(txns[0], 0, 0, 0, 0, 0);
    ExpectTableLockSetSize(txns[1], 0, 0, 0, 0, 0);
    ExpectTableLockSetSize(txns[2], 0, 0, 0, 0, 0);
  }));

  threads.push_back(std::thread([&]() {
    lock_manager.AcquireTableLock(table_name, LockMode::S, txns[1]);
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    lock_manager.ReleaseTableLock(table_name, LockMode::S, txns[1]);
    ExpectTableLockSetSize(txns[0], 0, 0, 0, 0, 0);
    ExpectTableLockSetSize(txns[1], 0, 0, 0, 0, 0);
    ExpectTableLockSetSize(txns[2], 1, 0, 0, 0, 0);
    txn_manager.Commit(txns[1]);
  }));

  threads.push_back(std::thread([&]() {
    lock_manager.AcquireTableLock(table_name, LockMode::S, txns[2]);
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    lock_manager.ReleaseTableLock(table_name, LockMode::S, txns[2]);
    ExpectTableLockSetSize(txns[1], 0, 0, 0, 0, 0);
    ExpectTableLockSetSize(txns[2], 0, 0, 0, 0, 0);
    txn_manager.Commit(txns[2]);
  }));

  for (auto &thread : threads) {
    thread.join();
  }
}

TEST(LockManagerTest, LivenessTest1) {
  auto dummy_storage = SetUpDummyStorage();
  auto txn_manager = TxnManager(dummy_storage);
  auto &lock_manager = txn_manager.GetLockManager();
  std::string tab = "table0";
  int txn_num = 5;
  std::vector<Txn *> txns;
  for (int i = 0; i < txn_num; ++i) {
    txns.push_back(txn_manager.Begin());
  }
  std::vector<std::thread> threads;
  // old wait for young.
  for (int i = txn_num - 1; i >= 0; --i) {
    threads.push_back(std::thread([i, &tab, &txns, &lock_manager]() {
      lock_manager.AcquireTableLock(tab, LockMode::X, txns[i]);
    }));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  // FIFO order.
  for (int i = txn_num - 1; i >= 0; --i) {
    ExpectTableLockSetSize(txns[i], 0, 1, 0, 0, 0);
    for (int j = txn_num - 1; j >= 0; --j) {
      if (j != i) {
        ExpectTableLockSetSize(txns[j], 0, 0, 0, 0, 0);
      }
    }
    lock_manager.ReleaseTableLock(tab, LockMode::X, txns[i]);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  for (auto &thread : threads) {
    thread.join();
  }
}

TEST(LockManagerTest, LivenessTest2) {
  auto dummy_storage = SetUpDummyStorage();
  auto txn_manager = TxnManager(dummy_storage);
  auto &lock_manager = txn_manager.GetLockManager();
  std::string tab = "table0";
  int txn_num = 5;
  std::vector<Txn *> txns;
  for (int i = 0; i < txn_num; ++i) {
    txns.push_back(txn_manager.Begin());
  }
  std::vector<std::thread> threads;
  lock_manager.AcquireTableLock(tab, LockMode::X, txns[txn_num - 1]);
  // old wait for young.
  for (int i = txn_num - 2; i >= 0; --i) {
    threads.push_back(std::thread([i, &tab, &txns, &lock_manager]() {
      lock_manager.AcquireTableLock(tab, LockMode::S, txns[i]);
    }));
  }
  lock_manager.ReleaseTableLock(tab, LockMode::X, txns[txn_num - 1]);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  for (int i = txn_num - 2; i >= 0; --i) {
    ExpectTableLockSetSize(txns[i], 1, 0, 0, 0, 0);
  }
  for (auto &thread : threads) {
    thread.join();
  }
}

/*
    T0                T1
    X(table2)         X(table1)
    X(table1)->wait   sleep 20ms
    WAIT              X(table2)->ABORT
    X(table1)         ABORT
    COMMIT

    T2                T3
                      X(table1)
                      X(table2)
    X(table2)->wait   sleep 50ms
    WAIT              COMMIT
    X(table2)
    X(table1)
    COMMIT
*/
TEST(LockManagerTest, WaitDieTest) {
  auto dummy_storage = SetUpDummyStorage();
  auto txn_manager = TxnManager(dummy_storage);
  auto &lock_manager = txn_manager.GetLockManager();
  std::string table1 = "table1";
  std::string table2 = "table2";

  /*----Begin die test----*/
  std::promise<void> t1done;
  std::shared_future<void> t1_future(t1done.get_future());
  Txn *t0 = txn_manager.Begin();  // old
  Txn *t1 = txn_manager.Begin();  // young

  auto thread_for_t1 = std::thread([&]() {
    lock_manager.AcquireTableLock(table1, LockMode::X, t1);
    EXPECT_EQ(t1->state_, TxnState::GROWING);
    ExpectTableLockSetSize(t1, 0, 1, 0, 0, 0);
    t1done.set_value();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_THROW(lock_manager.AcquireTableLock(table2, LockMode::X, t1),
        TxnDLAbortException);
    EXPECT_EQ(t1->state_, TxnState::ABORTED);
    txn_manager.Abort(t1);
    ExpectTableLockSetSize(t1, 0, 0, 0, 0, 0);
  });

  lock_manager.AcquireTableLock(table2, LockMode::X, t0);
  t1_future.wait();
  // this should wait
  lock_manager.AcquireTableLock(table1, LockMode::X, t0);
  EXPECT_EQ(t0->state_, TxnState::GROWING);
  ExpectTableLockSetSize(t0, 0, 2, 0, 0, 0);
  txn_manager.Commit(t0);
  EXPECT_EQ(t0->state_, TxnState::COMMITTED);
  ExpectTableLockSetSize(t0, 0, 0, 0, 0, 0);
  thread_for_t1.join();

  /*----Begin wait test----*/
  std::promise<void> t3done;
  std::shared_future<void> t3_future(t3done.get_future());
  Txn *t2 = txn_manager.Begin();
  Txn *t3 = txn_manager.Begin();

  auto thread_for_t3 = std::thread([&]() {
    lock_manager.AcquireTableLock(table1, LockMode::X, t3);
    EXPECT_EQ(t3->state_, TxnState::GROWING);
    ExpectTableLockSetSize(t3, 0, 1, 0, 0, 0);
    lock_manager.AcquireTableLock(table2, LockMode::X, t3);
    t3done.set_value();
    // t2 is waiting on t3 to release locks.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ExpectTableLockSetSize(t3, 0, 2, 0, 0, 0);
    ExpectTableLockSetSize(t2, 0, 0, 0, 0, 0);
    txn_manager.Commit(t3);
    ExpectTableLockSetSize(t3, 0, 0, 0, 0, 0);
  });

  t3_future.wait();
  // t2 should wait for t3 to release table2 lock
  lock_manager.AcquireTableLock(table2, LockMode::X, t2);
  ExpectTableLockSetSize(t3, 0, 0, 0, 0, 0);
  ExpectTableLockSetSize(t2, 0, 1, 0, 0, 0);
  lock_manager.AcquireTableLock(table1, LockMode::X, t2);
  ExpectTableLockSetSize(t2, 0, 2, 0, 0, 0);
  EXPECT_EQ(t2->state_, TxnState::GROWING);
  txn_manager.Commit(t2);
  EXPECT_EQ(t2->state_, TxnState::COMMITTED);
  ExpectTableLockSetSize(t2, 0, 0, 0, 0, 0);
  thread_for_t3.join();
}

// disabled
TEST(LockManagerTest, DISABLED_WoundWaitTest) {
  auto dummy_storage = SetUpDummyStorage();
  auto txn_manager =
      TxnManager(dummy_storage, LockManager::DL_Algorithm::WOUND_WAIT);
  auto &lock_manager = txn_manager.GetLockManager();
  std::string table1 = "table1";
  std::string table2 = "table2";

  /*----Begin wound test----*/
  std::promise<void> t1done;
  std::shared_future<void> t1_future(t1done.get_future());
  Txn *t0 = txn_manager.Begin();  // old
  Txn *t1 = txn_manager.Begin();  // young

  auto thread_for_t1 = std::thread([&]() {
    lock_manager.AcquireTableLock(table1, LockMode::X, t1);
    EXPECT_EQ(t1->state_, TxnState::GROWING);
    ExpectTableLockSetSize(t1, 0, 1, 0, 0, 0);
    t1done.set_value();
    EXPECT_THROW(lock_manager.AcquireTableLock(table2, LockMode::X, t1),
        TxnDLAbortException);
    txn_manager.Abort(t1);
    ExpectTableLockSetSize(t1, 0, 0, 0, 0, 0);
  });

  lock_manager.AcquireTableLock(table2, LockMode::X, t0);
  t1_future.wait();
  // sleep for t1 to wait on table2 lock.
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  lock_manager.AcquireTableLock(table1, LockMode::X, t0);
  EXPECT_EQ(t0->state_, TxnState::GROWING);
  ExpectTableLockSetSize(t0, 0, 2, 0, 0, 0);
  txn_manager.Commit(t0);
  EXPECT_EQ(t0->state_, TxnState::COMMITTED);
  ExpectTableLockSetSize(t0, 0, 0, 0, 0, 0);
  thread_for_t1.join();

  /*----Begin wait test----*/
  std::promise<void> t2done;
  std::shared_future<void> t2_future(t2done.get_future());
  Txn *t2 = txn_manager.Begin();
  Txn *t3 = txn_manager.Begin();

  auto thread_for_t2 = std::thread([&]() {
    lock_manager.AcquireTableLock(table1, LockMode::X, t2);
    EXPECT_EQ(t2->state_, TxnState::GROWING);
    ExpectTableLockSetSize(t2, 0, 1, 0, 0, 0);
    lock_manager.AcquireTableLock(table2, LockMode::X, t2);
    t2done.set_value();
    // t4 is waiting on t3 to release locks.
    ExpectTableLockSetSize(t2, 0, 2, 0, 0, 0);
    ExpectTableLockSetSize(t3, 0, 0, 0, 0, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    txn_manager.Commit(t2);
    ExpectTableLockSetSize(t2, 0, 0, 0, 0, 0);
  });

  t2_future.wait();
  // t3 should wait for t2 to release table2 lock
  lock_manager.AcquireTableLock(table2, LockMode::X, t3);
  ExpectTableLockSetSize(t2, 0, 0, 0, 0, 0);
  ExpectTableLockSetSize(t3, 0, 1, 0, 0, 0);
  lock_manager.AcquireTableLock(table1, LockMode::X, t3);
  ExpectTableLockSetSize(t3, 0, 2, 0, 0, 0);
  EXPECT_EQ(t3->state_, TxnState::GROWING);
  txn_manager.Commit(t3);
  EXPECT_EQ(t3->state_, TxnState::COMMITTED);
  ExpectTableLockSetSize(t3, 0, 0, 0, 0, 0);
  thread_for_t2.join();
}

TEST(QueryTest, SimpleAbortTest) {
  std::filesystem::remove("__tmp0100");
  auto db = std::make_unique<wing::Instance>("__tmp0100", SAKURA_USE_JIT_FLAG);
  auto &txn_manager = db->GetTxnManager();
  EXPECT_TRUE(db->Execute("create table Numbers(t varchar(30) primary key, a "
                          "int32, b float64);")
                  .Valid());
  auto txn1 = txn_manager.Begin();
  EXPECT_TRUE(db->Execute("insert into Numbers values ('blogaholic', 1, 2.3), "
                          " ('bookaholic', 2, 3.4), "
                          " ('alcoholic', 3, 4.5),"
                          " ('milkaholic', 4, 5.9),"
                          " ('foodaholic', 5, 9.7),"
                          " ('workaholic', 6, 10.99),"
                          " ('chocaholic', 7, 9.2),"
                          " ('lifeaholic', 8, 11.9),"
                          " ('laughaholic', 9, 7.0),"
                          " ('spendaholic', 10, 75.0);",
                    txn1->txn_id_)
                  .Valid());
  txn_manager.Abort(txn1);
  auto res = db->Execute("select * from Numbers;");
  EXPECT_TRUE(res.Valid());
  EXPECT_FALSE(res.Next());
}

TEST(QueryTest, RollbackTest) {
  std::filesystem::remove("__tmp0100");
  auto db = std::make_unique<wing::Instance>("__tmp0100", SAKURA_USE_JIT_FLAG);
  auto &txn_manager = db->GetTxnManager();
  EXPECT_TRUE(db->Execute("create table Numbers(t varchar(30) primary key, a "
                          "int32, b float64);")
                  .Valid());
  EXPECT_TRUE(db->Execute("insert into Numbers values ('blogaholic', 1, 2.3), "
                          " ('alcoholic', 3, 4.5);")
                  .Valid());
  auto txn1 = txn_manager.Begin();
  ExecUpdate(db.get(), "Numbers", "t", "blogaholic", format("{}, {}", 2, 2.3),
      txn1->txn_id_);
  db->Execute("delete from Numbers where t = 'alcoholic';", txn1->txn_id_);
  db->Execute(
      "insert into Numbers values ('bookaholic', 2, 3.4);", txn1->txn_id_);
  txn_manager.Abort(txn1);
  auto res = db->Execute("select * from Numbers order by t desc;");
  EXPECT_TRUE(res.Valid());
  auto res_data1 = res.Next();
  EXPECT_TRUE(res_data1);
  AnsMap<std::string> answer;
  answer.emplace("blogaholic",
      MkVec(SV::Create("blogaholic"), IV::Create(1), FV::Create(2.3)));
  answer.emplace("alcoholic",
      MkVec(SV::Create("alcoholic"), IV::Create(3), FV::Create(4.5)));
  EXPECT_TRUE(
      CheckAns(format("{}", res_data1.ReadString(0)), answer, res_data1, 3));
  auto res_data2 = res.Next();
  EXPECT_TRUE(res_data2);
  EXPECT_TRUE(
      CheckAns(format("{}", res_data2.ReadString(0)), answer, res_data2, 3));
}

TEST(AnomalyQueryTest, PhantomReadTest) {
  std::filesystem::remove("__tmp0100");
  auto db = std::make_unique<wing::Instance>("__tmp0100", SAKURA_USE_JIT_FLAG);
  auto &txn_manager = db->GetTxnManager();
  EXPECT_TRUE(db->Execute("create table Numbers(t varchar(30) primary key, a "
                          "int32, b float64);")
                  .Valid());
  auto txn1 = txn_manager.Begin();
  EXPECT_TRUE(db->Execute("insert into Numbers values ('blogaholic', 1, 2.3), "
                          " ('bookaholic', 2, 3.4);",
                    txn1->txn_id_)
                  .Valid());
  auto txn2 = txn_manager.Begin();
  // Self-abort because of phantom read.
  EXPECT_THROW(db->Execute("select * from Numbers;", txn2->txn_id_),
      TxnDLAbortException);
  txn_manager.Commit(txn1);
  txn_manager.Abort(txn2);
  auto txn3 = txn_manager.Begin();
  auto res = db->Execute("select * from Numbers;", txn3->txn_id_);
  EXPECT_TRUE(res.Valid());
  AnsMap<std::string> answer;
  answer.emplace("blogaholic",
      MkVec(SV::Create("blogaholic"), IV::Create(1), FV::Create(2.3)));
  answer.emplace("bookaholic",
      MkVec(SV::Create("bookaholic"), IV::Create(2), FV::Create(3.4)));
  for (uint32_t i = 0; i < answer.size(); i++) {
    auto tuple = res.Next();
    EXPECT_TRUE(bool(tuple));
    EXPECT_TRUE(CheckAns(format("{}", tuple.ReadString(0)), answer, tuple, 3));
  }
  EXPECT_FALSE(res.Next());
}

TEST(AnomalyQueryTest, DirtyReadTest) {
  std::filesystem::remove("__tmp0100");
  auto db = std::make_unique<wing::Instance>("__tmp0100", SAKURA_USE_JIT_FLAG);
  auto &txn_manager = db->GetTxnManager();
  EXPECT_TRUE(db->Execute("create table Numbers(t varchar(30) primary key, a "
                          "int32, b float64);")
                  .Valid());
  EXPECT_TRUE(db->Execute("insert into Numbers values ('blogaholic', 1, 2.3), "
                          " ('bookaholic', 2, 3.4);")
                  .Valid());
  auto txn1 = txn_manager.Begin();
  // logically equal to update
  EXPECT_TRUE(
      db->Execute("delete from Numbers where t='blogaholic';", txn1->txn_id_)
          .Valid());
  EXPECT_TRUE(db->Execute("insert into Numbers values ('blogaholic', 3, 4.3);",
                    txn1->txn_id_)
                  .Valid());
  auto txn2 = txn_manager.Begin();
  // Self-abort because of dirty read.
  EXPECT_THROW(
      db->Execute("select * from Numbers where t='blogaholic';", txn2->txn_id_),
      TxnDLAbortException);
  txn_manager.Commit(txn1);
  txn_manager.Abort(txn2);
  auto txn3 = txn_manager.Begin();
  auto res =
      db->Execute("select * from Numbers where t='blogaholic';", txn3->txn_id_);
  EXPECT_TRUE(res.Valid());
  AnsMap<std::string> answer;
  answer.emplace("blogaholic",
      MkVec(SV::Create("blogaholic"), IV::Create(3), FV::Create(4.3)));
  for (uint32_t i = 0; i < answer.size(); i++) {
    auto tuple = res.Next();
    EXPECT_TRUE(bool(tuple));
    EXPECT_TRUE(CheckAns(format("{}", tuple.ReadString(0)), answer, tuple, 3));
  }
  EXPECT_FALSE(res.Next());
}

auto InitWithTable(int init_balance, std::string file_name)
    -> std::unique_ptr<wing::Instance> {
  std::filesystem::remove(file_name);
  auto db = std::make_unique<wing::Instance>(file_name, SAKURA_USE_JIT_FLAG);
  auto &txn_manager = db->GetTxnManager();
  ExecAsATxnUntilCommit(
      db.get(), txn_manager, [&init_balance](Instance *db, txn_id_t txn_id) {
        EXPECT_TRUE(
            db->Execute("create table Numbers(t varchar(30) primary key, a "
                        "int32);",
                  txn_id)
                .Valid());
        EXPECT_TRUE(
            db->Execute(format("insert into Numbers values ('blogaholic', {});",
                            init_balance),
                  txn_id)
                .Valid());
      });
  return db;
}

TEST(AnomalyQueryTest, NonRepeatableReadTest) {
  uint32_t repetition_cnt = 10;
  int init_balance = 1000;
  for (uint32_t i = 0; i < repetition_cnt; i++) {
    auto db = InitWithTable(init_balance, "__tmp_NonRepeatableReadTest");
    auto &txn_manager = db->GetTxnManager();
    std::vector<std::thread> threads;
    threads.push_back(std::thread([&]() {
      // T1: read B, B = B + 100
      ExecAsATxnUntilCommit(
          db.get(), txn_manager, [](Instance *db, txn_id_t txn_id) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(std::rand() % 100));
            auto res = db->Execute(
                "select a from Numbers where t='blogaholic';", txn_id);
            EXPECT_TRUE(res.Valid());
            auto from = res.Next().ReadInt(0);
            ExecUpdate(db, "Numbers", "t", "blogaholic",
                format("{}", from + 100), txn_id);
          });
    }));
    threads.push_back(std::thread([&]() {
      // T2: read B, read B, B = B * 1.01
      ExecAsATxnUntilCommit(
          db.get(), txn_manager, [](Instance *db, txn_id_t txn_id) {
            EXPECT_TRUE(
                db->Execute(
                      "select a from Numbers where t='blogaholic';", txn_id)
                    .Valid());
            std::this_thread::sleep_for(
                std::chrono::milliseconds(std::rand() % 100));
            auto res = db->Execute(
                "select a from Numbers where t='blogaholic';", txn_id);
            EXPECT_TRUE(res.Valid());
            auto from = res.Next().ReadInt(0);
            ExecUpdate(db, "Numbers", "t", "blogaholic",
                format("{}", from * 1.01), txn_id);
          });
    }));

    for (auto &t : threads) {
      t.join();
    }
    auto res = db->Execute("select a from Numbers where t='blogaholic';")
                   .Next()
                   .ReadInt(0);
    EXPECT_TRUE(res == (init_balance * 1.01 + 100) ||
                res == (init_balance + 100) * 1.01);
  }
}

TEST(AnomalyQueryTest, LostUpdateTest) {
  uint32_t repetition_cnt = 10;
  int init_balance = 1000;
  for (uint32_t i = 0; i < repetition_cnt; i++) {
    auto db = InitWithTable(init_balance, "__tmp_LostUpdateTest");
    auto &txn_manager = db->GetTxnManager();
    std::vector<std::thread> threads;
    threads.push_back(std::thread([&]() {
      // T1: read A, A = A + 100
      ExecAsATxnUntilCommit(
          db.get(), txn_manager, [](Instance *db, txn_id_t txn_id) {
            auto res = db->Execute(
                "select a from Numbers where t='blogaholic';", txn_id);
            EXPECT_TRUE(res.Valid());
            auto from = res.Next().ReadInt(0);
            std::this_thread::sleep_for(
                std::chrono::milliseconds(std::rand() % 100));
            ExecUpdate(db, "Numbers", "t", "blogaholic",
                format("{}", from + 100), txn_id);
          });
    }));
    threads.push_back(std::thread([&]() {
      // T2: read A, A = A + 200
      ExecAsATxnUntilCommit(
          db.get(), txn_manager, [](Instance *db, txn_id_t txn_id) {
            auto res = db->Execute(
                "select a from Numbers where t='blogaholic';", txn_id);
            EXPECT_TRUE(res.Valid());
            auto from = res.Next().ReadInt(0);
            std::this_thread::sleep_for(
                std::chrono::milliseconds(std::rand() % 100));
            ExecUpdate(db, "Numbers", "t", "blogaholic",
                format("{}", from + 200), txn_id);
          });
    }));

    for (auto &t : threads) {
      t.join();
    }
    auto res = db->Execute("select a from Numbers where t='blogaholic';")
                   .Next()
                   .ReadInt(0);
    EXPECT_TRUE(res == init_balance + 300);
  }
}

TEST(AnomalyQueryTest, WriteSkewTest) {
  uint32_t repetition_cnt = 10;
  int init_balance = 1;
  for (uint32_t i = 0; i < repetition_cnt; ++i) {
    auto db = InitWithTable(init_balance, "__tmp_WriteSkewTest");
    auto &txn_manager = db->GetTxnManager();
    for (int j = 0; j < 10; ++j) {
      EXPECT_TRUE(
          db->Execute(format("insert into Numbers values ('row{}', {});", j + 1,
                          j % 2))
              .Valid());
    }
    std::vector<std::thread> threads;
    threads.push_back(std::thread([&]() {
      // T1: change all values that are 1 to 0
      ExecAsATxnUntilCommit(
          db.get(), txn_manager, [](Instance *db, txn_id_t txn_id) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(std::rand() % 10));
            auto res = db->Execute("select t from Numbers where a=1;", txn_id);
            EXPECT_TRUE(res.Valid());
            for (auto res_data = res.Next(); res_data != false;
                 res_data = res.Next()) {
              auto t = res_data.ReadString(0);
              ExecUpdate(db, "Numbers", "t", t, format("{}", 0), txn_id);
            }
          });
    }));
    threads.push_back(std::thread([&]() {
      // T2: change all values that are 0 to 1
      ExecAsATxnUntilCommit(
          db.get(), txn_manager, [](Instance *db, txn_id_t txn_id) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(std::rand() % 10));
            auto res = db->Execute("select t from Numbers where a=0;", txn_id);
            EXPECT_TRUE(res.Valid());
            for (auto res_data = res.Next(); res_data != false;
                 res_data = res.Next()) {
              auto t = res_data.ReadString(0);
              ExecUpdate(db, "Numbers", "t", t, format("{}", 1), txn_id);
            }
          });
    }));

    for (auto &t : threads) {
      t.join();
    }
    // all 1 or all 0
    auto res_set = db->Execute("select * from Numbers;");
    auto res_data_0 = res_set.Next();
    for (auto res_data = res_data_0; res_data != false;
         res_data = res_set.Next()) {
      EXPECT_TRUE(res_data.ReadInt(1) == res_data_0.ReadInt(1));
    }
  }
}

// Transfer money example in slide.
void TransferMoneyTest(uint32_t repetition_cnt, uint32_t txn_cnt) {
  uint32_t init_balance = std::max(10000U, txn_cnt);
  for (uint32_t i = 0; i < repetition_cnt; i++) {
    std::filesystem::remove("__tmp_TransferMoneyTest");
    auto db = std::make_unique<wing::Instance>(
        "__tmp_TransferMoneyTest", SAKURA_USE_JIT_FLAG);
    auto &txn_manager = db->GetTxnManager();
    ExecAsATxnUntilCommit(
        db.get(), txn_manager, [&init_balance](Instance *db, txn_id_t txn_id) {
          EXPECT_TRUE(
              db->Execute("create table Numbers(t varchar(30) primary key, a "
                          "int32);",
                    txn_id)
                  .Valid());
          EXPECT_TRUE(
              db->Execute(
                    format("insert into Numbers values ('blogaholic', {}), "
                           " ('alcoholic', {});",
                        init_balance, init_balance),
                    txn_id)
                  .Valid());
        });
    // Each txn transfers 1 from blogaholic to alcoholic. Total txn_cnt txns.
    std::vector<std::thread> threads;
    for (uint32_t i = 0; i < txn_cnt; i++) {
      threads.push_back(std::thread([&]() {
        ExecAsATxnUntilCommit(
            db.get(), txn_manager, [](Instance *db, txn_id_t txn_id) {
              auto res = db->Execute(
                  "select a from Numbers where t='blogaholic';", txn_id);
              EXPECT_TRUE(res.Valid());
              auto from = res.Next().ReadInt(0);
              ExecUpdate(db, "Numbers", "t", "blogaholic",
                  format("{}", from - 1), txn_id);
              res = db->Execute(
                  "select a from Numbers where t='alcoholic';", txn_id);
              EXPECT_TRUE(res.Valid());
              auto to = res.Next().ReadInt(0);
              ExecUpdate(db, "Numbers", "t", "alcoholic", format("{}", to + 1),
                  txn_id);
            });
      }));
    }
    for (auto &t : threads) {
      t.join();
    }
    auto res = db->Execute("select a from Numbers where t='blogaholic';")
                   .Next()
                   .ReadInt(0);
    EXPECT_EQ(res, init_balance - txn_cnt);
    res = db->Execute("select a from Numbers where t='alcoholic';")
              .Next()
              .ReadInt(0);
    EXPECT_EQ(res, init_balance + txn_cnt);
  }
}

TEST(ConcurrentQueryTest, TransferMoneyTest1_R10T10) {
  TransferMoneyTest(10, 10);
}
TEST(ConcurrentQueryTest, TransferMoneyTest1_R10T50) {
  TransferMoneyTest(10, 50);
}
TEST(ConcurrentQueryTest, TransferMoneyTest1_R10T100) {
  TransferMoneyTest(10, 100);
}
TEST(ConcurrentQueryTest, TransferMoneyTest1_R100T100) {
  TransferMoneyTest(100, 100);
}

void TransferMoneyTestThreePeople(uint32_t repetition_cnt, uint32_t txn_cnt) {
  uint32_t init_balance = std::max(10000U, txn_cnt);
  for (uint32_t i = 0; i < repetition_cnt; i++) {
    std::filesystem::remove("__tmp_TransferMoneyTest");
    auto db = std::make_unique<wing::Instance>(
        "__tmp_TransferMoneyTest", SAKURA_USE_JIT_FLAG);
    auto &txn_manager = db->GetTxnManager();
    ExecAsATxnUntilCommit(
        db.get(), txn_manager, [&init_balance](Instance *db, txn_id_t txn_id) {
          EXPECT_TRUE(
              db->Execute("create table Numbers(t varchar(30) primary key, a "
                          "int32);",
                    txn_id)
                  .Valid());
          EXPECT_TRUE(
              db->Execute(
                    format("insert into Numbers values ('blogaholic', {}), "
                           " ('alcoholic', {}), ('chocaholic', {});",
                        init_balance, init_balance, init_balance),
                    txn_id)
                  .Valid());
        });
    // Each txn1 transfers 1 from alcoholic to blogaholic.
    // Each txn2 transfers 2 from chocaholic to blogaholic.
    std::vector<std::thread> threads;
    for (uint32_t i = 0; i < txn_cnt; i++) {
      threads.push_back(std::thread([&]() {
        ExecAsATxnUntilCommit(
            db.get(), txn_manager, [](Instance *db, txn_id_t txn_id) {
              auto res = db->Execute(
                  "select a from Numbers where t='alcoholic';", txn_id);
              EXPECT_TRUE(res.Valid());
              auto from = res.Next().ReadInt(0);
              ExecUpdate(db, "Numbers", "t", "alcoholic",
                  format("{}", from - 1), txn_id);

              res = db->Execute(
                  "select a from Numbers where t='blogaholic';", txn_id);
              EXPECT_TRUE(res.Valid());
              auto to = res.Next().ReadInt(0);
              ExecUpdate(db, "Numbers", "t", "blogaholic", format("{}", to + 1),
                  txn_id);
            });
      }));

      threads.push_back(std::thread([&]() {
        ExecAsATxnUntilCommit(
            db.get(), txn_manager, [](Instance *db, txn_id_t txn_id) {
              auto res = db->Execute(
                  "select a from Numbers where t='chocaholic';", txn_id);
              EXPECT_TRUE(res.Valid());
              auto from = res.Next().ReadInt(0);
              ExecUpdate(db, "Numbers", "t", "chocaholic",
                  format("{}", from - 2), txn_id);

              res = db->Execute(
                  "select a from Numbers where t='blogaholic';", txn_id);
              EXPECT_TRUE(res.Valid());
              auto to = res.Next().ReadInt(0);
              ExecUpdate(db, "Numbers", "t", "blogaholic", format("{}", to + 2),
                  txn_id);
            });
      }));
    }
    for (auto &t : threads) {
      t.join();
    }
    auto res = db->Execute("select a from Numbers where t='alcoholic';")
                   .Next()
                   .ReadInt(0);
    EXPECT_EQ(res, init_balance - txn_cnt);
    res = db->Execute("select a from Numbers where t='blogaholic';")
              .Next()
              .ReadInt(0);
    EXPECT_EQ(res, init_balance + txn_cnt * 3);
    res = db->Execute("select a from Numbers where t='chocaholic';")
              .Next()
              .ReadInt(0);
    EXPECT_EQ(res, init_balance - txn_cnt * 2);
  }
}

TEST(ConcurrentQueryTest, TransferMoneyTest2_R1T1) {
  TransferMoneyTestThreePeople(1, 1);
}
TEST(ConcurrentQueryTest, TransferMoneyTest2_R10T50) {
  TransferMoneyTestThreePeople(10, 50);
}
TEST(ConcurrentQueryTest, TransferMoneyTest2_R10T100) {
  TransferMoneyTestThreePeople(10, 100);
}
TEST(ConcurrentQueryTest, TransferMoneyTest2_R50T100) {
  TransferMoneyTestThreePeople(50, 100);
}

constexpr int THREAD_CNT = 4;       // 4 CPUs on Autolab's machine
constexpr int BENCH_DURATION = 15;  // in seconds

TEST(TxnBenchmark, BenchmarkTable) {
  constexpr int TOTAL_TABLE_CNT = 10000;
  const char *pk_name = "a";
  const char *pk_value = "v";

  std::mt19937_64 gen(0x202305152105);
  zipf_distribution<> zipf(TOTAL_TABLE_CNT);

  std::uniform_int_distribution<int> uniform_dist(0, TOTAL_TABLE_CNT - 1);

  std::filesystem::remove("__tmp_BenchTest");
  auto db =
      std::make_unique<wing::Instance>("__tmp_BenchTest", SAKURA_USE_JIT_FLAG);
  auto &txn_manager = db->GetTxnManager();
  for (int i = 0; i < TOTAL_TABLE_CNT; ++i) {
    EXPECT_TRUE(
        db->Execute(
              format("create table t{}(a varchar(2) primary key, b int32);", i))
            .Valid());
    EXPECT_TRUE(
        db->Execute(format("insert into t{} values ('v', 1);", i)).Valid());
  }
  std::vector<std::thread> threads;
  std::atomic<uint64_t> commit_cnt = 0;
  std::atomic<uint64_t> abort_cnt = 0;
  auto start_time = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < THREAD_CNT; ++i) {
    threads.push_back(std::thread([&]() {
      uint64_t local_commit_cnt = 0;
      uint64_t local_abort_cnt = 0;
      while (true) {
        auto txn = txn_manager.Begin();
        try {
          ExecUpdate(db.get(), format("t{}", zipf(gen) - 1), pk_name, pk_value,
              format("{}", uniform_dist(gen)), txn->txn_id_);
          txn_manager.Commit(txn);
          local_commit_cnt++;
        } catch (TxnDLAbortException &e) {
          txn_manager.Abort(txn);
          local_abort_cnt++;
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } catch (MultiUpgradeException &e) {
          txn_manager.Abort(txn);
          local_abort_cnt++;
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        auto now = std::chrono::high_resolution_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - start_time)
                .count() > BENCH_DURATION) {
          break;
        }
      }
      commit_cnt += local_commit_cnt;
      abort_cnt += local_abort_cnt;
    }));
  }
  for (auto &t : threads) {
    t.join();
  }
  for (int i = 0; i < TOTAL_TABLE_CNT; ++i) {
    auto res = db->Execute(format("select count(*) from t{};", i));
    EXPECT_TRUE(res.Valid());
    EXPECT_TRUE(res.Next().ReadInt(0) == 1);
  }
  std::cout << "Total commit cnt: " << commit_cnt << std::endl;
  std::cout << "Total abort cnt: " << abort_cnt << std::endl;
  std::filesystem::remove("__txn_benchmark_result1");
  std::ofstream out("__txn_benchmark_result1");
  out << commit_cnt;
}

TEST(TxnBenchmark, BenchmarkTuple) {
  constexpr int TOTAL_TUPLE_CNT = 10000;
  const char *tab_name = "t";
  const char *pk_name = "a";

  std::mt19937_64 gen(0x202305152135);
  zipf_distribution<> zipf(TOTAL_TUPLE_CNT);

  std::uniform_int_distribution<int> uniform_dist(0, TOTAL_TUPLE_CNT - 1);

  std::filesystem::remove("__tmp_BenchTest");
  auto db =
      std::make_unique<wing::Instance>("__tmp_BenchTest", SAKURA_USE_JIT_FLAG);
  auto &txn_manager = db->GetTxnManager();
  EXPECT_TRUE(
      db->Execute(format("create table t(a int32 primary key, b int32);"))
          .Valid());
  for (int i = 0; i < TOTAL_TUPLE_CNT; ++i) {
    EXPECT_TRUE(
        db->Execute(format("insert into t values ({}, 1);", i)).Valid());
  }
  std::vector<std::thread> threads;
  std::atomic<uint64_t> commit_cnt = 0;
  std::atomic<uint64_t> abort_cnt = 0;
  auto start_time = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < THREAD_CNT; ++i) {
    threads.push_back(std::thread([&]() {
      uint64_t local_commit_cnt = 0;
      uint64_t local_abort_cnt = 0;
      while (true) {
        auto txn = txn_manager.Begin();
        try {
          ExecUpdate<false>(db.get(), tab_name, pk_name,
              format("{}", zipf(gen) - 1), format("{}", uniform_dist(gen)),
              txn->txn_id_);
          txn_manager.Commit(txn);
          local_commit_cnt++;
        } catch (TxnDLAbortException &e) {
          txn_manager.Abort(txn);
          local_abort_cnt++;
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } catch (MultiUpgradeException &e) {
          txn_manager.Abort(txn);
          local_abort_cnt++;
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        auto now = std::chrono::high_resolution_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - start_time)
                .count() > BENCH_DURATION) {
          break;
        }
      }
      commit_cnt += local_commit_cnt;
      abort_cnt += local_abort_cnt;
    }));
  }
  for (auto &t : threads) {
    t.join();
  }
  auto res = db->Execute("select count(*) from t;");
  EXPECT_TRUE(res.Valid());
  EXPECT_TRUE(res.Next().ReadInt(0) == TOTAL_TUPLE_CNT);
  std::cout << "Total commit cnt: " << commit_cnt << std::endl;
  std::cout << "Total abort cnt: " << abort_cnt << std::endl;
  std::filesystem::remove("__txn_benchmark_result2");
  std::ofstream out("__txn_benchmark_result2");
  out << commit_cnt;
}
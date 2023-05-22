#ifndef SAKURA_DB_H__
#define SAKURA_DB_H__

#include <chrono>

#include "catalog/gen_pk.hpp"
#include "catalog/schema.hpp"
#include "catalog/stat.hpp"
#include "storage/storage.hpp"
#include "transaction/txn.hpp"
#include "transaction/txn_manager.hpp"

namespace wing {

class DB {
 public:
  DB(std::string_view file_name);

  ~DB();

  /* Create a table using table schema. Table name is stored in table schema. */
  void CreateTable(txn_id_t txn_id, const TableSchema& schema);

  /* Drop table table_name. */
  void DropTable(txn_id_t txn_id, std::string_view table_name);

  /** Get the iterator. It returns an iterator pointing to the beginning of the
   * table. txn_id: the transaction id. Used for logging and concurrency
   * control. table_name: the table.
   */
  std::unique_ptr<Iterator<const uint8_t*>> GetIterator(
      txn_id_t txn_id, std::string_view table_name);

  /** Get the range iterator. It returns an iterator pointing to the leftmost
   * element in the interval [L, R] or (L, R) or (L, R] or [L, R) or [L, inf) or
   * (L, inf) or (-inf, R) or (-inf, R] or (-inf, inf) The iterator ensures that
   * it only returns elements within this interval.
   *
   * Parameter L, R: the tuple of (key, is_empty, is_eq).
   * If is_empty is true, then it doesn't have limit in one direction. If is_eq
   * is true. then the endpoint of the interval is closed.
   */
  std::unique_ptr<Iterator<const uint8_t*>> GetRangeIterator(txn_id_t txn_id,
      std::string_view table_name, std::tuple<std::string_view, bool, bool> L,
      std::tuple<std::string_view, bool, bool> R);

  /* Get a handle for modifying table. See storage.hpp for definition of
   * ModifyHandle. */
  std::unique_ptr<ModifyHandle> GetModifyHandle(
      txn_id_t txn_id, std::string_view table_name);

  /* Get a handle for doing some searching operation. See storage.hpp for
   * definition of SearchHandle. */
  std::unique_ptr<SearchHandle> GetSearchHandle(
      txn_id_t txn_id, std::string_view table_name);

  // Generate auto_increment keys (i.e. primary key)
  GenPKHandle GetGenPKHandle(txn_id_t txn_id, std::string_view table_name);

  // Update the statistics of table_name with a new table statistics.
  void UpdateStats(std::string_view table_name, TableStatistics&& stat);

  // Return the pointer to the statistic data. Return null if there is no stats.
  const TableStatistics* GetTableStat(std::string_view table_name) const;

  const DBSchema& GetDBSchema() const;

  TxnManager& GetTxnManager();

  // Used for generating referred table name. These tables are used for storing
  // refcounts of primary key.
  static std::string GenRefTableName(std::string_view table_name) {
    return fmt::format("__refcounts_of_{}", table_name);
  }

  // Used for generating column name in referred table.
  static std::string GenRefColumnName(std::string_view pk_name) {
    return fmt::format("{}_refcounts", pk_name);
  }

  // Used for generating default primary key name. Some tables don't define
  // primary key. So we have to generate one.
  static std::string GenDefaultPKName() {
    return fmt::format("__default_primary_key_{}",
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
  }

 private:
  class Impl;
  std::unique_ptr<Impl> ptr_;
};

}  // namespace wing

#endif
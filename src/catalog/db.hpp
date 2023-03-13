#ifndef SAKURA_DB_H__
#define SAKURA_DB_H__

#include <chrono>

#include "catalog/gen_pk.hpp"
#include "catalog/schema.hpp"
#include "catalog/stat.hpp"
#include "storage/storage.hpp"

namespace wing {

class DB {
 public:
  DB(std::string_view file_name);

  ~DB();

  void CreateTable(const TableSchema& schema);

  void DropTable(std::string_view table_name);

  std::unique_ptr<Iterator<const uint8_t*>> GetIterator(size_t txn_id, std::string_view table_name);

  std::unique_ptr<Iterator<const uint8_t*>> GetRangeIterator(size_t txn_id, std::string_view table_name,
                                                             std::pair<std::string_view, bool> L,
                                                             std::pair<std::string_view, bool> R);

  std::unique_ptr<ModifyHandle> GetModifyHandle(size_t txn_id, std::string_view table_name);

  std::unique_ptr<SearchHandle> GetSearchHandle(size_t txn_id, std::string_view table_name);

  // Generate auto_increment keys (i.e. primary key)
  GenPKHandle GetGenPKHandle(size_t txn_id, std::string_view table_name);

  void UpdateStats(std::string_view table_name, TableStatistics&& stat);

  // Return the pointer to the statistic data. Return null if there is no stats.
  const TableStatistics* GetTableStat(std::string_view table_name) const;

  const DBSchema& GetDBSchema() const;

  // Generate a transaction id.
  // Acquires a read lock to metadata.
  size_t GenerateTxnID();

  // If the transaction is metadata operation such as
  // create/drop table/index.
  // Acquires a write lock to the metadata.
  void AcquireMetadataWLock(size_t txn_id);

  // Release the read lock to the metadata.
  void ReleaseMetadataLock(size_t txn_id);

  void Rollback(size_t txn_id);

  void Commit(size_t txn_id);

  static std::string GenRefTableName(std::string_view table_name) { return fmt::format("__refcounts_of_{}", table_name); }

  static std::string GenRefColumnName(std::string_view pk_name) { return fmt::format("{}_refcounts", pk_name); }

  static std::string GenDefaultPKName() {
    return fmt::format("__default_primary_key_{}", std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
  }

 private:
  class Impl;
  std::unique_ptr<Impl> ptr_;
};

}  // namespace wing

#endif
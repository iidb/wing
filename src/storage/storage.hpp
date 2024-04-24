#pragma once

#include <memory>
#include <vector>

#include "catalog/schema.hpp"
#include "transaction/lock_manager.hpp"

namespace wing {

/**
 * Iterator. Read a set of rows.
 * Iterator is read only.
 * Ensure that return value (e.g. const uint8_t*) is valid until the next Next()
 * is called.
 */
template <typename TupleType>
class Iterator {
 public:
  virtual ~Iterator() = default;
  virtual void Init() = 0;
  virtual TupleType Next() = 0;
};

/**
 * ModifyHandle. Modify a set.
 * View it as a KV store. (key, value) cannot be duplicate.
 * Key can be duplicate. For example, in a secondary index.
 */
class ModifyHandle {
 public:
  virtual ~ModifyHandle() = default;
  virtual void Init() = 0;
  virtual bool Delete(std::string_view key) = 0;
  virtual bool Insert(std::string_view key, std::string_view value) = 0;
  virtual bool Update(std::string_view key, std::string_view new_value) = 0;
};

/**
 * SearchHandle. Search a row by key.
 * SearchHandle is read only.
 * Used in index scan.
 * Ensure that return value (e.g. const uint8_t*) is valid until the next
 * Search() is called.
 *
 * You can store additional information to speed up query. For example, you can
 * cache 1 page so that you don't need to fetch pages if the next query hits the
 * page. But you cannot use too much memory, for example, you cannot save the
 * whole B+tree in memory.
 */
class SearchHandle {
 public:
  virtual ~SearchHandle() = default;
  virtual void Init() = 0;
  virtual const uint8_t* Search(std::string_view key) = 0;
};

class Storage {
 public:
  virtual ~Storage() = default;

  virtual std::unique_ptr<ModifyHandle> GetModifyHandle(
      std::unique_ptr<TxnExecCtx> ctx) = 0;

  virtual std::unique_ptr<SearchHandle> GetSearchHandle(
      std::unique_ptr<TxnExecCtx> ctx) = 0;

  virtual void Create(const TableSchema& schema) = 0;

  virtual void Drop(std::string_view table_name) = 0;

  virtual std::unique_ptr<Iterator<const uint8_t*>> GetIterator(
      std::string_view table_name) = 0;

  virtual std::unique_ptr<Iterator<const uint8_t*>> GetRangeIterator(
      std::string_view table_name, std::tuple<std::string_view, bool, bool> L,
      std::tuple<std::string_view, bool, bool> R) = 0;

  virtual std::optional<std::string_view> GetMaxKey(
      std::string_view table_name) = 0;

  virtual size_t GetTicks(std::string_view table_name) = 0;

  virtual const DBSchema& GetDBSchema() const = 0;
};

}  // namespace wing

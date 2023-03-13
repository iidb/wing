#ifndef SAKURA_FK_CHECKER_H__
#define SAKURA_FK_CHECKER_H__

#include <vector>

#include "catalog/db.hpp"
#include "catalog/schema.hpp"
#include "execution/exprdata.hpp"
#include "storage/storage.hpp"
#include "type/static_field.hpp"
#include "type/tuple.hpp"

namespace wing {
/**
 * (F)oreign(K)eyChecker. Used to (1) check if referred key exists. (2) update the refcounts.
 */
class FKChecker {
 public:
  FKChecker(const std::vector<ForeignKeySchema>& fks, const TableSchema& table, size_t txn_id, DB& db) : fk_schema_(fks) {
    for (auto& a : fks) {
      auto tab = a.table_name_;
      auto ref_tab = DB::GenRefTableName(tab);
      fk_check_.push_back(db.GetSearchHandle(txn_id, tab));
      fk_check_in_refcounts_.push_back(db.GetSearchHandle(txn_id, ref_tab));
      fk_update_refcounts_.push_back(db.GetModifyHandle(txn_id, ref_tab));
      fk_offsets_.push_back(Tuple::GetOffset(table.GetShuffleToStorage()[a.index_], table.GetStorageColumns()));
    }
  }

  void Init() {
    for (auto& a : fk_check_) a->Init();
    for (auto& a : fk_check_in_refcounts_) a->Init();
    for (auto& a : fk_update_refcounts_) a->Init();
  }

  void InsertCheck(InputTuplePtr x) {
    for (uint32_t i = 0; i < fk_schema_.size(); i++) {
      auto key = x.Read<StaticFieldRef>(fk_schema_[i].index_ * sizeof(StaticFieldRef));
      auto key_view = StaticFieldRef::GetView(&key, fk_schema_[i].type_, fk_schema_[i].size_);
      if (auto ret = fk_check_in_refcounts_[i]->Search(key_view); ret) {
        size_t value = InputTuplePtr(ret).Read<size_t>(Tuple::GetOffsetOfStaticField(0)) + 1;
        _update(i, key, fk_schema_[i].type_, fk_schema_[i].size_, key_view, value, false);
        continue;
      }
      if (!fk_check_[i]->Search(key_view)) {
        throw DBException("Primary key does not exist.");
      }
      // Create a new entry.
      _update(i, key, fk_schema_[i].type_, fk_schema_[i].size_, key_view, 1, true);
    }
  }

  void InsertCommit(InputTuplePtr raw_x) {
    for (uint32_t i = 0; i < fk_schema_.size(); i++) {
      auto key_view = Tuple::GetFieldView(raw_x.Data(), fk_offsets_[i], fk_schema_[i].type_, fk_schema_[i].size_);
      auto key = StaticFieldRef::CreateFromStringView(key_view, fk_schema_[i].type_);
      if (auto ret = fk_check_in_refcounts_[i]->Search(key_view); ret) {
        size_t value = InputTuplePtr(ret).Read<size_t>(Tuple::GetOffsetOfStaticField(0)) + 1;
        _update(i, key, fk_schema_[i].type_, fk_schema_[i].size_, key_view, value, false);
        continue;
      }
      if (!fk_check_[i]->Search(key_view)) {
        throw DBException("Primary key does not exist.");
      }
      // Create a new entry.
      _update(i, key, fk_schema_[i].type_, fk_schema_[i].size_, key_view, 1, true);
    }
  }

  void DeleteCheck(InputTuplePtr raw_x) {
    for (uint32_t i = 0; i < fk_schema_.size(); i++) {
      auto key_view = Tuple::GetFieldView(raw_x.Data(), fk_offsets_[i], fk_schema_[i].type_, fk_schema_[i].size_);
      auto key = StaticFieldRef::CreateFromStringView(key_view, fk_schema_[i].type_);
      if (auto ret = fk_check_in_refcounts_[i]->Search(key_view); ret) {
        size_t value = InputTuplePtr(ret).Read<size_t>(Tuple::GetOffsetOfStaticField(0));
        if (value == 0) {
          throw DBException("Refcounts becomes negative.");
        }
        value = value - 1;
        if (!value) {
          fk_update_refcounts_[i]->Delete(key_view);
        } else {
          _update(i, key, fk_schema_[i].type_, fk_schema_[i].size_, key_view, value, false);
        }
      } else {
        // This case is incorrect. If this tuple was inserted before, then ref_table should have the referred key.
        throw DBException("Referred primary key was removed.");
      }
    }
  }

 private:
  void _update(uint32_t i, StaticFieldRef key, FieldType key_type, uint32_t key_size, std::string_view key_view, size_t new_value, bool is_insert) {
    StaticFieldRef value[2];
    value[0] = StaticFieldRef::CreateInt(new_value);
    value[1] = key;
    std::array<ColumnSchema, 2> cs{
      ColumnSchema("", FieldType::INT64, 8),
      ColumnSchema("", key_type, key_size)};
    uint32_t shu[2] = {0, 1};
    size_t size = Tuple::GetSerializeSize(value, cs);
    // Tuple is not big.
    char data[size];
    Tuple::Serialize(data, value, cs, shu);
    !is_insert ? fk_update_refcounts_[i]->Update(key_view, {data, size}) : fk_update_refcounts_[i]->Insert(key_view, {data, size});
  }
  const std::vector<ForeignKeySchema>& fk_schema_;
  std::vector<uint32_t> fk_offsets_;
  std::vector<std::unique_ptr<SearchHandle>> fk_check_;
  std::vector<std::unique_ptr<SearchHandle>> fk_check_in_refcounts_;
  std::vector<std::unique_ptr<ModifyHandle>> fk_update_refcounts_;
};

}  // namespace wing

#endif
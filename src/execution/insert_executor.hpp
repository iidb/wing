#ifndef SAKURA_INSERT_EXECUTOR_H__
#define SAKURA_INSERT_EXECUTOR_H__

#include "catalog/db.hpp"
#include "catalog/gen_pk.hpp"
#include "execution/executor.hpp"
#include "execution/fk_checker.hpp"
#include "type/tuple.hpp"

namespace wing {

class InsertExecutor : public Executor {
 public:
  InsertExecutor(std::unique_ptr<ModifyHandle>&& handle,
      std::unique_ptr<Executor> ch, FKChecker checker, GenPKHandle gen_pk,
      const TableSchema& table_schema)
    : handle_(std::move(handle)),
      ch_(std::move(ch)),
      gen_pk_(gen_pk),
      fk_checker_(std::move(checker)),
      table_schema_(table_schema) {
    pk_index_ = table_schema_.GetPrimaryKeyIndex();
    pk_offset_ = Tuple::GetOffset(table_schema_.GetStoragePrimaryKeyIndex(),
        table_schema_.GetStorageColumns());
    pk_type_ = table_schema_.GetPrimaryKeySchema().type_;
    pk_size_ = table_schema_.GetPrimaryKeySchema().size_;
  }
  void Init() override {
    handle_->Init();
    ch_->Init();
    fk_checker_.Init();
    done_flag_ = false;
    insert_row_counts_.data_.int_data = 0;
    if (table_schema_.GetHidePKFlag()) {
      temp_.resize(table_schema_.GetColumns().size());
    }
  }
  InputTuplePtr Next() override {
    if (done_flag_) {
      return {};
    }
    done_flag_ = true;
    auto ch_ret = ch_->Next();
    // TODO: if ch_ is PrintExecutor then we don't need to insert after read all
    // tuples. We should check the constraints and raise DBExeceptions before
    // inserting into table.
    while (ch_ret) {
      // If the primary key is hidden, then we insert a '0' at the position
      // (default at the end).
      if (table_schema_.GetHidePKFlag()) {
        auto p = table_schema_.GetPrimaryKeyIndex();
        for (uint32_t i = 0; i < p; i++) {
          temp_[i] = ch_ret.Read<StaticFieldRef>(i * sizeof(StaticFieldRef));
        }
        temp_[p] = StaticFieldRef::CreateInt(0);
        for (uint32_t i = p; i < table_schema_.GetColumns().size() - 1; i++) {
          temp_[i + 1] =
              ch_ret.Read<StaticFieldRef>(i * sizeof(StaticFieldRef));
        }
        fk_checker_.InsertCheck(temp_);
        insert_rows_.push_back(Serialize(temp_));
      } else {
        fk_checker_.InsertCheck(ch_ret);
        insert_rows_.push_back(Serialize(ch_ret));
      }
      ch_ret = ch_->Next();
    }
    // Release the iterator
    ch_ = nullptr;
    // Insert the tuples
    for (auto& row : insert_rows_) {
      auto key_view =
          Tuple::GetFieldView(row.data(), pk_offset_, pk_type_, pk_size_);
      if (!handle_->Insert(key_view, row)) {
        throw DBException("Insert error: duplicate key!");
      }
    }
    insert_row_counts_.data_.int_data = insert_rows_.size();
    return reinterpret_cast<const uint8_t*>(&insert_row_counts_);
  }

 private:
  std::string_view Serialize(InputTuplePtr input) {
    auto size =
        Tuple::GetSerializeSize(input.Data(), table_schema_.GetColumns());
    auto data_ptr = data_.Allocate(size);
    Tuple::Serialize(data_ptr, input.Data(), table_schema_.GetStorageColumns(),
        table_schema_.GetShuffleFromStorage());
    // Since InputTuplePtr is const, we cannot modify it. Instead, we modify
    // serialized result. Auto_increment keys are integers. so we directly use
    // pk_offset_.
    if (gen_pk_) {
      // Generate only when the value is 0. (MySQL grammar)
      auto pk = input.Read<StaticFieldRef>(pk_index_ * sizeof(StaticFieldRef))
                    .ReadInt();
      if (pk == 0) {
        StaticFieldRef::CreateInt(gen_pk_.Gen())
            .Write(pk_type_, pk_size_, data_ptr + pk_offset_);
      }
    }
    return {reinterpret_cast<char*>(data_ptr), size};
  }
  std::unique_ptr<ModifyHandle> handle_;
  std::unique_ptr<Executor> ch_;
  GenPKHandle gen_pk_;
  FKChecker fk_checker_;

  const TableSchema& table_schema_;
  uint32_t pk_index_;
  uint32_t pk_offset_;
  FieldType pk_type_;
  uint32_t pk_size_;

  bool done_flag_{false};
  StaticFieldRef insert_row_counts_;

  BlockAllocator<8192> data_;
  std::vector<std::string_view> insert_rows_;

  std::vector<StaticFieldRef> temp_;
};

}  // namespace wing

#endif
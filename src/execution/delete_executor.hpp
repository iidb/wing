#ifndef SAKURA_DELETE_EXECUTOR_H__
#define SAKURA_DELETE_EXECUTOR_H__

#include "execution/executor.hpp"
#include "execution/fk_checker.hpp"
#include "execution/pk_checker.hpp"

namespace wing {

class DeleteExecutor : public Executor {
 public:
  DeleteExecutor(std::unique_ptr<ModifyHandle>&& handle, std::unique_ptr<Executor> ch, FKChecker fk_checker, PKChecker pk_checker,
                 const TableSchema& table_schema)
      : handle_(std::move(handle)), ch_(std::move(ch)), fk_checker_(std::move(fk_checker)), pk_checker_(std::move(pk_checker)) {
    pk_offset_ = Tuple::GetOffset(table_schema.GetStoragePrimaryKeyIndex(), table_schema.GetStorageColumns());
    pk_type_ = table_schema.GetPrimaryKeySchema().type_;
    pk_size_ = table_schema.GetPrimaryKeySchema().size_;
  }
  void Init() override {
    handle_->Init();
    ch_->Init();
    fk_checker_.Init();
    done_flag_ = false;
    delete_row_counts_.data_.int_data = 0;
  }

  InputTuplePtr Next() override {
    if (done_flag_) {
      return {};
    }
    done_flag_ = true;
    auto ch_ret = ch_->Next();
    // Get the primary keys of all the obsolete tuples.
    // Input is raw tuple data.
    while (ch_ret) {
      // Check foreign keys and primary keys.
      fk_checker_.DeleteCheck(ch_ret);
      auto pk_view = Tuple::GetFieldView(ch_ret.Data(), pk_offset_, pk_type_, pk_size_);
      pk_checker_.DeleteCheck(pk_view);
      // Allocate a block to store tuple.
      auto pk_data_ptr = data_.Allocate(pk_view.size());
      std::memcpy(pk_data_ptr, pk_view.data(), pk_view.size());
      obsolete_tuple_primary_keys_.push_back({reinterpret_cast<const char*>(pk_data_ptr), pk_view.size()});
      delete_row_counts_.data_.int_data++;
      ch_ret = ch_->Next();
    }
    // Release the iterator.
    ch_ = nullptr;
    // Delete the tuples.
    for (auto& a : obsolete_tuple_primary_keys_) {
      if (!handle_->Delete(a)) {
        throw DBException("Delete operation failed.");
      }
    }
    return reinterpret_cast<const uint8_t*>(&delete_row_counts_);
  }

 private:
  std::unique_ptr<ModifyHandle> handle_;
  std::unique_ptr<Executor> ch_;
  FKChecker fk_checker_;
  PKChecker pk_checker_;

  BlockAllocator<8192> data_;
  std::vector<std::string_view> obsolete_tuple_primary_keys_;

  StaticFieldRef delete_row_counts_;

  uint32_t pk_offset_;
  FieldType pk_type_;
  uint32_t pk_size_;

  bool done_flag_{false};
};

}  // namespace wing

#endif
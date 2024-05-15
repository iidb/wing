#pragma once

#include "execution/executor.hpp"

namespace wing {

class PrintVecExecutor : public VecExecutor {
 public:
  PrintVecExecutor(const ExecOptions& options,
      std::shared_ptr<StaticFieldArray> vec, const OutputSchema& input_schema,
      size_t num_fields)
    : VecExecutor(options),
      vec_(vec),
      num_fields_(num_fields),
      schema_(input_schema) {
    DB_ASSERT(
        num_fields_ != 0 && vec_->GetFieldVector().size() % num_fields_ == 0);
    size_ = vec_->GetFieldVector().size();
    ptr_ = vec_->GetFieldVector().data();
  }
  void Init() override {
    offset_ = 0;
    tuples_.Init(schema_.GetTypes(), max_batch_size_);
  }
  TupleBatch InternalNext() override {
    if (offset_ >= size_) {
      return {};
    } else {
      tuples_.Clear();
      while (offset_ < size_ && tuples_.size() < tuples_.Capacity()) {
        auto ret = ptr_ + offset_;
        offset_ += num_fields_;
        tuples_.Append(std::span<const StaticFieldRef>(ret, num_fields_));
      }
      return tuples_;
    }
  }

 private:
  std::shared_ptr<StaticFieldArray> vec_;
  size_t num_fields_{0};
  OutputSchema schema_;
  size_t offset_{0};
  size_t size_;
  const StaticFieldRef* ptr_;
  TupleBatch tuples_;
};

}  // namespace wing

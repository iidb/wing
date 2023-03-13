#ifndef SAKURA_PRINT_EXECUTOR_H__
#define SAKURA_PRINT_EXECUTOR_H__

#include "execution/executor.hpp"

namespace wing {

class PrintExecutor : public Executor {
 public:
  PrintExecutor(std::shared_ptr<StaticFieldVector> vec, size_t num_fields) : vec_(vec), num_fields_(num_fields) {
    DB_ASSERT(num_fields_ != 0 && vec_->GetFieldVector().size() % num_fields_ == 0);
    size_ = vec_->GetFieldVector().size();
    ptr_ = vec_->GetFieldVector().data();
  }
  void Init() override { offset_ = 0; }
  InputTuplePtr Next() override {
    if (offset_ >= size_) {
      return {};
    } else {
      auto ret = ptr_ + offset_;
      offset_ += num_fields_;
      return ret;
    }
  }

 private:
  std::shared_ptr<StaticFieldVector> vec_;
  size_t num_fields_{0};
  size_t offset_{0};
  size_t size_;
  const StaticFieldRef* ptr_;
};

}  // namespace wing

#endif
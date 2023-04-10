#ifndef SAKURA_PROJECT_EXECUTOR_H__
#define SAKURA_PROJECT_EXECUTOR_H__

#include "execution/executor.hpp"

namespace wing {

class ProjectExecutor : public Executor {
 public:
  ProjectExecutor(const std::vector<std::unique_ptr<Expr>>& exprs,
      const OutputSchema& input_schema, std::unique_ptr<Executor> ch)
    : ch_(std::move(ch)) {
    data_.reserve(exprs.size());
    for (auto& a : exprs)
      data_.push_back(ExprFunction(a.get(), input_schema));
  }
  void Init() override {
    result_.resize(data_.size());
    ch_->Init();
  }
  InputTuplePtr Next() override {
    if (auto ch_ret = ch_->Next(); ch_ret) {
      for (uint32_t i = 0; i < data_.size(); i++) {
        // For INT and FLOAT results, numbers are stored directly in FieldRef,
        // so it is safe. For STRING results, since expressions do not generate
        // new strings, strings are from literal strings (which are stored in
        // std::function) or string columns. We ensure that the btree page we
        // are using will not be modified or flushed. So that the reference is
        // valid.
        result_[i] = data_[i].Evaluate(ch_ret);
      }
      return result_;
    } else {
      return {};
    }
  }

 private:
  std::vector<ExprFunction> data_;
  std::vector<StaticFieldRef> result_;
  std::unique_ptr<Executor> ch_;
};
}  // namespace wing

#endif
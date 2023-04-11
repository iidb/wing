#ifndef SAKURA_FILTER_EXECUTOR_H__
#define SAKURA_FILTER_EXECUTOR_H__

#include "execution/executor.hpp"

namespace wing {

class FilterExecutor : public Executor {
 public:
  FilterExecutor(const std::unique_ptr<Expr>& expr,
      const OutputSchema& input_schema, std::unique_ptr<Executor> ch)
    : predicate_(ExprFunction(expr.get(), input_schema)), ch_(std::move(ch)) {}
  void Init() override { ch_->Init(); }
  InputTuplePtr Next() override {
    auto ch_ret = ch_->Next();
    while (
        ch_ret && (predicate_ && predicate_.Evaluate(ch_ret).ReadInt() == 0))
      ch_ret = ch_->Next();
    return ch_ret;
  }

 private:
  ExprFunction predicate_;
  std::unique_ptr<Executor> ch_;
};

}  // namespace wing

#endif
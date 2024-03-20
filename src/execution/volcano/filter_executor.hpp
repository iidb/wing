#pragma once

#include "execution/executor.hpp"

namespace wing {

class FilterExecutor : public Executor {
 public:
  FilterExecutor(const std::unique_ptr<Expr>& expr,
      const OutputSchema& input_schema, std::unique_ptr<Executor> ch)
    : predicate_(ExprExecutor(expr.get(), input_schema)), ch_(std::move(ch)) {}
  void Init() override { ch_->Init(); }
  SingleTuple Next() override {
    auto ch_ret = ch_->Next();
    while (ch_ret && (predicate_ && predicate_.Evaluate(ch_ret).ReadInt() == 0))
      ch_ret = ch_->Next();
    return ch_ret;
  }

 private:
  ExprExecutor predicate_;
  std::unique_ptr<Executor> ch_;
};

}  // namespace wing

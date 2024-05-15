#pragma once

#include "execution/executor.hpp"
#include "execution/vec/expr_vexecutor.hpp"

namespace wing {

class FilterVecExecutor : public VecExecutor {
 public:
  FilterVecExecutor(const ExecOptions& options,
      const std::unique_ptr<Expr>& expr, const OutputSchema& input_schema,
      std::unique_ptr<VecExecutor> ch)
    : VecExecutor(options),
      pred_(ExprVecExecutor::Create(expr.get(), input_schema)),
      ch_(std::move(ch)) {}
  void Init() override { ch_->Init(); }
  TupleBatch InternalNext() override {
    auto ch_ret = ch_->Next();
    if (ch_ret.size() == 0) {
      return {};
    }
    if (pred_) {
      pred_.Evaluate(ch_ret.GetCols(), ch_ret.size(), pred_result_);
      for (uint32_t i = 0; i < ch_ret.size(); i++)
        if (pred_result_.Get(i).ReadInt() == 0) {
          ch_ret.SetValid(i, false);
        }
    }
    return ch_ret;
  }

 private:
  ExprVecExecutor pred_;
  Vector pred_result_;
  std::unique_ptr<VecExecutor> ch_;
};

}  // namespace wing

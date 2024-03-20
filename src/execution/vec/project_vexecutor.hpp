#pragma once

#include "execution/executor.hpp"
#include "execution/vec/expr_vexecutor.hpp"

namespace wing {

class ProjectVecExecutor : public VecExecutor {
 public:
  ProjectVecExecutor(const std::vector<std::unique_ptr<Expr>>& exprs,
      const OutputSchema& input_schema, std::unique_ptr<VecExecutor> ch)
    : ch_(std::move(ch)) {
    exprs_.reserve(exprs.size());
    for (auto& expr : exprs) {
      exprs_.emplace_back(ExprVecExecutor::Create(expr.get(), input_schema));
    }
  }
  void Init() override {
    expr_results_.resize(exprs_.size());
    ch_->Init();
  }
  TupleBatch Next() override {
    if (auto ch_ret = ch_->Next(); ch_ret.size() > 0) {
      for (uint32_t id = 0; id < exprs_.size(); id++) {
        exprs_[id].Evaluate(ch_ret.GetCols(), ch_ret.size(), expr_results_[id]);
      }
      TupleBatch ret;
      ret.Init(expr_results_, ch_ret.size(), ch_ret.GetSelVector());
      return ret;
    } else {
      return {};
    }
  }

 private:
  std::vector<ExprVecExecutor> exprs_;
  std::vector<Vector> expr_results_;
  std::unique_ptr<VecExecutor> ch_;
};
}  // namespace wing

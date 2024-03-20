#pragma once

#include <functional>
#include <span>

#include "execution/volcano/expr_executor.hpp"
#include "parser/expr.hpp"
#include "plan/output_schema.hpp"
#include "type/static_field.hpp"
#include "type/tuple_batch.hpp"

namespace wing {

class ExprVecExecutor {
 public:
  void Init();

  void Evaluate(std::span<Vector> input, size_t count, Vector& result);

  static ExprVecExecutor Create(
      const Expr* expr, const OutputSchema& input_schema);

  operator bool() const { return IsValid(); }

  bool IsValid() const { return bool(func_); }

 private:
  class CreateInternalState {
   public:
    size_t agg_id_{0};
    std::vector<ExprVecExecutor> aggs_;
    std::vector<std::pair<std::string, LogicalType>> agg_metadata_;
  };
  static ExprVecExecutor CreateInternal(const Expr* expr,
      const OutputSchema& input_schema, CreateInternalState& state);

 private:
  std::function<int(std::span<Vector>, size_t, Vector&)> func_;
  std::vector<ExprVecExecutor> ch_;
  std::vector<Vector> imm_;

  friend class AggExprVecExecutor;
};

class AggExprVecExecutor {
 public:
  void Init();

  void Aggregate(AggIntermediateData* data, TupleBatch::SingleTuple input);

  AggIntermediateData* CreateAggData();

  void EvaluateAggParas(
      std::span<Vector> input, size_t count, std::vector<Vector>& result);

  void FinalEvaluate(std::span<AggIntermediateData*> data,
      std::span<Vector> input, Vector& result);

  static AggExprVecExecutor Create(
      const Expr* expr, const OutputSchema& input_schema);

  operator bool() const { return IsValid(); }

  bool IsValid() const { return expr_.IsValid(); }

 private:
  ExprVecExecutor expr_;
  std::vector<ExprVecExecutor> agg_para_;
  std::vector<std::function<void(AggIntermediateData&, StaticFieldRef)>>
      agg_func_;
  std::vector<std::function<void(std::span<AggIntermediateData*>, Vector&)>>
      agg_final_func_;
  ArenaAllocator alloc_;
};

}  // namespace wing

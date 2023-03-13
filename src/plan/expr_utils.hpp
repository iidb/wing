#ifndef SAKURA_EXPR_UTILS_H__
#define SAKURA_EXPR_UTILS_H__

#include <vector>

#include "parser/expr.hpp"
#include "plan/output_schema.hpp"
#include "common/bitvector.hpp"

namespace wing {
class ExprUtils {
 public:
  static void DivideIntoPredicateList(std::unique_ptr<Expr> expr, std::vector<std::unique_ptr<Expr>>& result) {
    if (expr->type_ == ExprType::BINCONDOP && static_cast<BinaryConditionExpr*>(expr.get())->op_ == OpType::AND) {
      auto this_expr = static_cast<BinaryConditionExpr*>(expr.get());
      DivideIntoPredicateList(std::move(this_expr->ch0_), result);
      DivideIntoPredicateList(std::move(this_expr->ch1_), result);
    } else {
      std::vector<std::unique_ptr<Expr>> vec;
      result.push_back(std::move(expr));
    }
  }

  static std::vector<std::unique_ptr<Expr>> DivideIntoPredicateList(std::unique_ptr<Expr> expr) {
    std::vector<std::unique_ptr<Expr>> ret;
    DivideIntoPredicateList(std::move(expr), ret);
    return ret;
  }

  static void GetExprIds(const Expr* expr, std::vector<uint32_t>& result) {
    if (expr->type_ == ExprType::COLUMN) {  
      result.push_back(static_cast<const ColumnExpr*>(expr)->id_table_in_planner_);
    } else {
      if (expr->ch0_ != nullptr) GetExprIds(expr->ch0_.get(), result);
      if (expr->ch1_ != nullptr) GetExprIds(expr->ch1_.get(), result);
    }
  }

  static BitVector GetExprBitVector(const Expr* expr) {
    std::vector<uint32_t> list;
    GetExprIds(expr, list);
    if (list.size() == 0) {
      return BitVector();
    }
    BitVector ret(*std::max_element(list.begin(), list.end()) + 1);
    for (auto a : list) ret[a] = 1;
    return ret;
  }

  // Should ensure that there is no nested aggregate functions.
  static std::unique_ptr<Expr> ApplyExprOnExpr(const Expr* expr, const std::vector<std::unique_ptr<Expr>>& input_exprs,
                                               const OutputSchema& input_schema) {
    if (expr->type_ == ExprType::BINCONDOP || expr->type_ == ExprType::BINOP) {
      auto L = ApplyExprOnExpr(expr->ch0_.get(), input_exprs, input_schema);
      auto R = ApplyExprOnExpr(expr->ch1_.get(), input_exprs, input_schema);
      std::unique_ptr<Expr> ret;
      if (expr->type_ == ExprType::BINOP) {
        ret = std::make_unique<BinaryExpr>(static_cast<const BinaryExpr*>(expr)->op_, std::move(L), std::move(R));
      } else {
        ret = std::make_unique<BinaryConditionExpr>(static_cast<const BinaryConditionExpr*>(expr)->op_, std::move(L), std::move(R));
      }
      ret->ret_type_ = expr->ret_type_;
      return ret;
    } else if (expr->type_ == ExprType::UNARYOP || expr->type_ == ExprType::UNARYCONDOP || expr->type_ == ExprType::CAST) {
      auto L = ApplyExprOnExpr(expr->ch0_.get(), input_exprs, input_schema);
      std::unique_ptr<Expr> ret;
      if (expr->type_ == ExprType::UNARYOP) {
        ret = std::make_unique<UnaryExpr>(static_cast<const UnaryExpr*>(expr)->op_, std::move(L));
      } else if (expr->type_ == ExprType::UNARYCONDOP) {
        ret = std::make_unique<UnaryConditionExpr>(static_cast<const UnaryConditionExpr*>(expr)->op_, std::move(L));
      } else if (expr->type_ == ExprType::CAST) {
        ret = std::make_unique<CastExpr>(std::move(L));
      }
      ret->ret_type_ = expr->ret_type_;
      return ret;
    } else if (expr->type_ == ExprType::AGGR) {
      auto L = ApplyExprOnExpr(expr->ch0_.get(), input_exprs, input_schema);
      auto ret = std::make_unique<AggregateFunctionExpr>(static_cast<const AggregateFunctionExpr*>(expr)->func_name_, std::move(L));
      ret->ret_type_ = expr->ret_type_;
      return ret;
    } else if (expr->type_ == ExprType::COLUMN) {
      auto i = input_schema.FindById(static_cast<const ColumnExpr*>(expr)->id_in_column_name_table_);
      // i should have value.
      DB_ASSERT(i.has_value() && i.value() < input_exprs.size());
      return input_exprs[i.value()]->clone();
    } else {
      return expr->clone();
    }
  }
};
}  // namespace wing

#endif
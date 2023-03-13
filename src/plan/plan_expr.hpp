#ifndef SAKURA_PLAN_EXPR_H__
#define SAKURA_PLAN_EXPR_H__

#include "common/bitvector.hpp"
#include "parser/expr.hpp"
#include "plan/expr_utils.hpp"

namespace wing {

/**
 * expr_: Expressions from predicate. For example, for predicate (a = 0) and (b = 0 or c = 0) and (d > 0).
 * The expressions are a = 0, (b = 0 or c = 0) != 0, d > 0.
 * left_bits_: The table bitset of left expression.
 * right_bits_: The table bitset of right expression. 
 */
class PredicateElement {
 public:
  std::unique_ptr<BinaryConditionExpr> expr_;
  BitVector left_bits_;
  BitVector right_bits_;
  bool CheckLeft(const BitVector& v) const {
    return left_bits_.Check(v);
  }
  bool CheckRight(const BitVector& v) const {
    return right_bits_.Check(v);
  }
};

class PredicateVec {
 public:
  static PredicateVec Create(const Expr* expr) {
    if (!expr) {
      return PredicateVec();
    }
    auto list = ExprUtils::DivideIntoPredicateList(expr->clone());
    PredicateVec ret;
    for (auto&& a : list) {
      if (a->type_ == ExprType::BINCONDOP) {
        auto expr = _trans(std::move(a));
        auto left_bits_ = ExprUtils::GetExprBitVector(expr->ch0_.get());
        auto right_bits_ = ExprUtils::GetExprBitVector(expr->ch1_.get());
        ret.vec_.push_back(PredicateElement{std::move(expr), left_bits_, right_bits_});
      } else {
        auto expr = std::make_unique<BinaryConditionExpr>(OpType::NEQ, std::move(a), std::make_unique<LiteralIntegerExpr>(0));
        expr->ret_type_ = RetType::INT;
        auto left_bits_ = ExprUtils::GetExprBitVector(expr->ch0_.get());
        ret.vec_.push_back(PredicateElement{std::move(expr), left_bits_, BitVector()});
      }
    }
    return ret;
  }
  std::unique_ptr<Expr> GenExpr() const {
    if (!vec_.size()) {
      return nullptr;
    }
    std::unique_ptr<Expr> ret;
    ret = vec_[0].expr_->clone();
    for (uint32_t i = 1; i < vec_.size(); i++) {
      ret = std::make_unique<BinaryConditionExpr>(OpType::AND, std::move(ret), vec_[i].expr_->clone());
      ret->ret_type_ = RetType::INT;
    }
    return ret;
  }
  std::vector<std::unique_ptr<Expr>> GenLeftExprList() const {
    std::vector<std::unique_ptr<Expr>> ret;
    for (auto& a : vec_) ret.push_back(a.expr_->ch0_->clone());
    return ret;
  }
  std::vector<std::unique_ptr<Expr>> GenRightExprList() const {
    std::vector<std::unique_ptr<Expr>> ret;
    for (auto& a : vec_) ret.push_back(a.expr_->ch1_->clone());
    return ret;
  }

  std::vector<PredicateElement>& GetVec() { return vec_; }

  const std::vector<PredicateElement>& GetVec() const { return vec_; }

  PredicateVec clone() const {
    PredicateVec ret;
    ret.vec_.reserve(vec_.size());
    for (auto& a : vec_) {
      ret.vec_.push_back({_trans(a.expr_->clone()), a.left_bits_, a.right_bits_});
    }
    return ret;
  }

  std::string ToString() const {
    std::string ret;
    for (uint32_t i = 0; i < vec_.size(); i++) {
      ret += vec_[i].expr_->ToString();
      if (i != vec_.size() - 1) {
        ret += " AND ";
      }
    }
    return ret;
  }

  void Append(const PredicateVec& v) {
    for (auto& a : v.vec_) {
      vec_.push_back({_trans(a.expr_->clone()), a.left_bits_, a.right_bits_});
    }
  }

  void Append(PredicateVec&& v) { vec_.insert(vec_.end(), std::make_move_iterator(v.vec_.begin()), std::make_move_iterator(v.vec_.end())); }

  void Append(PredicateElement element) { vec_.push_back(std::move(element)); }

  void ApplyExpr(const std::vector<std::unique_ptr<Expr>>& input_exprs, const OutputSchema& input_schema) {
    for (auto&& a : vec_) {
      auto expr = _trans(ExprUtils::ApplyExprOnExpr(a.expr_.get(), input_exprs, input_schema));
      auto left_bits = ExprUtils::GetExprBitVector(expr->ch0_.get());
      auto right_bits = ExprUtils::GetExprBitVector(expr->ch1_.get());
      a.expr_ = std::move(expr);
      a.left_bits_ = std::move(left_bits);
      a.right_bits_ = std::move(right_bits);
    }
  }

 private:
  static std::unique_ptr<BinaryConditionExpr> _trans(std::unique_ptr<Expr> e) {
    auto ret = static_cast<BinaryConditionExpr*>(e.get());
    e.release();
    return std::unique_ptr<BinaryConditionExpr>(ret);
  }
  std::vector<PredicateElement> vec_;
};

}  // namespace wing

#endif
#ifndef SAKURA_CONVERT_TO_HASH_JOIN_H__
#define SAKURA_CONVERT_TO_HASH_JOIN_H__

#include "plan/rules/rule.hpp"

namespace wing {

/**
 *
 * Joins are initially NestloopJoins. But for some joins we can transform it to
 * hash join. These joins have at least one predicate that the two expressions
 * only use data from one table. For example. select * from A, B where A.a =
 * B.a;
 *
 * For the following SQL statement, 'A.a * 2 + A.b' and 'B.c' will be used as
 * hash keys. Although 'A.a + B.b = 1' can be transformed to 'A.a = 1 - B.b', we
 * don't consider that (This may be done by other rules.) select * from A, B
 * where A.a * 2 + A.b = B.c and A.a + B.b = 1;
 *
 * If there are multiple equalities, all of them will be used as hash keys.
 * For the following SQL statement, hash keys for A are 'A.a' and 'A.b', for B
 * are 'B.a' and 'B.b'. select * from A, B where A.a = B.a and A.b = B.b;
 *
 */
class ConvertToHashJoinRule : public OptRule {
 public:
  bool Match(const PlanNode* node) {
    if (node->type_ == PlanType::Join) {
      auto t_node = static_cast<const JoinPlanNode*>(node);
      for (auto& a : t_node->predicate_.GetVec()) {
        if (a.expr_->op_ == OpType::EQ) {
          if (!a.CheckRight(t_node->ch_->table_bitset_) &&
              !a.CheckLeft(t_node->ch2_->table_bitset_) &&
              a.CheckRight(t_node->ch2_->table_bitset_) &&
              a.CheckLeft(t_node->ch_->table_bitset_)) {
            return true;
          }
          if (!a.CheckLeft(t_node->ch_->table_bitset_) &&
              !a.CheckRight(t_node->ch2_->table_bitset_) &&
              a.CheckRight(t_node->ch_->table_bitset_) &&
              a.CheckLeft(t_node->ch2_->table_bitset_)) {
            return true;
          }
        }
      }
      return false;
    }
    return false;
  }
  std::unique_ptr<PlanNode> Transform(std::unique_ptr<PlanNode> node) {
    auto t_node = static_cast<JoinPlanNode*>(node.get());
    auto ret = std::make_unique<HashJoinPlanNode>();
    for (auto&& a : t_node->predicate_.GetVec()) {
      if (a.expr_->op_ == OpType::EQ) {
        if (!a.CheckRight(t_node->ch_->table_bitset_) &&
            !a.CheckLeft(t_node->ch2_->table_bitset_) &&
            a.CheckRight(t_node->ch2_->table_bitset_) &&
            a.CheckLeft(t_node->ch_->table_bitset_)) {
          ret->left_hash_exprs_.push_back(a.expr_->ch0_->clone());
          ret->right_hash_exprs_.push_back(a.expr_->ch1_->clone());
        } else if (!a.CheckLeft(t_node->ch_->table_bitset_) &&
                   !a.CheckRight(t_node->ch2_->table_bitset_) &&
                   a.CheckRight(t_node->ch_->table_bitset_) &&
                   a.CheckLeft(t_node->ch2_->table_bitset_)) {
          ret->right_hash_exprs_.push_back(a.expr_->ch0_->clone());
          ret->left_hash_exprs_.push_back(a.expr_->ch1_->clone());
        }
      }
      ret->predicate_.Append(std::move(a));
    }
    ret->ch_ = std::move(t_node->ch_);
    ret->ch2_ = std::move(t_node->ch2_);
    ret->output_schema_ = std::move(node->output_schema_);
    ret->table_bitset_ = std::move(node->table_bitset_);
    return ret;
  }
};

}  // namespace wing

#endif
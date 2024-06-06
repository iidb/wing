#pragma once

#include "plan/rules/rule.hpp"

namespace wing {

/**
 * Try to push down some predicates. For example,
 *  select * from A, B where A.a = B.a and A.b = 0
 * We can push A.b = 0 down.
 *
 * It supports HashJoin and Join.
 *
 */
class PushDownJoinPredicateRule : public OptRule {
 public:
  bool Match(const PlanNode* node) override {
    auto match_func = [](auto t_node) {
      for (auto& a : t_node->predicate_.GetVec()) {
        if (a.CheckRight(t_node->ch2_->table_bitset_) &&
            a.CheckLeft(t_node->ch2_->table_bitset_)) {
          return true;
        }
        if (a.CheckRight(t_node->ch_->table_bitset_) &&
            a.CheckLeft(t_node->ch_->table_bitset_)) {
          return true;
        }
      }
      return false;
    };
    if (node->type_ == PlanType::Join) {
      auto t_node = static_cast<const JoinPlanNode*>(node);
      return match_func(t_node);
    } else if (node->type_ == PlanType::HashJoin) {
      auto t_node = static_cast<const HashJoinPlanNode*>(node);
      return match_func(t_node);
    }

    return false;
  }
  std::unique_ptr<PlanNode> Transform(std::unique_ptr<PlanNode> node) override {
    auto trans_func = [](auto t_node) {
      PredicateVec left_v, right_v, nw_v;
      for (auto&& a : t_node->predicate_.GetVec()) {
        if (a.CheckRight(t_node->ch_->table_bitset_) &&
            a.CheckLeft(t_node->ch_->table_bitset_)) {
          left_v.Append(std::move(a));
        } else if (a.CheckRight(t_node->ch2_->table_bitset_) &&
                   a.CheckLeft(t_node->ch2_->table_bitset_)) {
          right_v.Append(std::move(a));
        } else {
          nw_v.Append(std::move(a));
        }
      }
      t_node->predicate_ = std::move(nw_v);

      if (left_v.GetVec().size()) {
        auto lfilter = std::make_unique<FilterPlanNode>();
        lfilter->predicate_ = std::move(left_v);
        lfilter->ch_ = std::move(t_node->ch_);
        lfilter->output_schema_ = lfilter->ch_->output_schema_;
        lfilter->table_bitset_ = lfilter->ch_->table_bitset_;
        t_node->ch_ = std::move(lfilter);
      }

      if (right_v.GetVec().size()) {
        auto rfilter = std::make_unique<FilterPlanNode>();
        rfilter->predicate_ = std::move(right_v);
        rfilter->ch_ = std::move(t_node->ch2_);
        rfilter->output_schema_ = rfilter->ch_->output_schema_;
        rfilter->table_bitset_ = rfilter->ch_->table_bitset_;
        t_node->ch2_ = std::move(rfilter);
      }
    };

    if (node->type_ == PlanType::Join) {
      auto t_node = static_cast<JoinPlanNode*>(node.get());
      trans_func(t_node);
    } else if (node->type_ == PlanType::HashJoin) {
      auto t_node = static_cast<HashJoinPlanNode*>(node.get());
      trans_func(t_node);
    }

    return node;
  }
};

}  // namespace wing

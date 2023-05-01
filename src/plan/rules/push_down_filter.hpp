#ifndef SAKURA_PUSHDOWN_FILTER_H__
#define SAKURA_PUSHDOWN_FILTER_H__

#include "common/logging.hpp"
#include "plan/expr_utils.hpp"
#include "plan/output_schema.hpp"
#include "plan/rules/rule.hpp"

namespace wing {

/**
 * FilterPlanNodes are generated on the top of Subqueries, Tables and Joins.
 * For example,
 * select * from (select * from A where A.b = 1) where A.a = 1;
 *               Filter [A.a = 1]
 *                     |
 *                  Project
 *                     |
 *               Filter [A.b = 1]
 *                     |
 *              SeqScan [Table: A]
 * We should swap Project and the first Filter, and combine the two Filters.
 * Another example,
 * select * from (select sum(a) as c from A) where c < 100;
 *               Filter [c < 100]
 *                     |
 *         Aggregate [Group predicate: NULL]
 *                     |
 *                 SeqScan [Table: A]
 * We should put the filter predicate into the group predicate of the aggregate
 * node.
 *
 * Filters can be swapped with Project, OrderBy, Distinct, SeqScan.
 * Filters should be combined with other Filters. This is done by this OptLRule.
 * Filters can be combined with the predicate in Aggregate and Join.
 * Filters cannot be swapped with Limit.
 */
class PushDownFilterRule : public OptRule {
 public:
  bool Match(const PlanNode* node) override {
    if (node->type_ == PlanType::Filter) {
      auto t_node = static_cast<const FilterPlanNode*>(node);
      // t_node->ch_ should be non-null.
      if (t_node->ch_->type_ == PlanType::Project ||
          t_node->ch_->type_ == PlanType::Aggregate ||
          t_node->ch_->type_ == PlanType::Order ||
          t_node->ch_->type_ == PlanType::Distinct ||
          t_node->ch_->type_ == PlanType::Filter ||
          t_node->ch_->type_ == PlanType::Join ||
          t_node->ch_->type_ == PlanType::SeqScan || 
          t_node->ch_->type_ == PlanType::HashJoin || 
          t_node->ch_->type_ == PlanType::RangeScan) {
        return true;
      }
    }
    return false;
  }
  std::unique_ptr<PlanNode> Transform(std::unique_ptr<PlanNode> node) override {
    auto t_node = static_cast<FilterPlanNode*>(node.get());
    if (t_node->ch_->type_ == PlanType::Distinct ||
        t_node->ch_->type_ == PlanType::Order) {
      auto ch = std::move(t_node->ch_);
      t_node->ch_ = std::move(ch->ch_);
      ch->ch_ = std::move(node);
      return ch;
    } else if (t_node->ch_->type_ == PlanType::Filter) {
      auto ch = std::move(t_node->ch_);
      auto t_ch = static_cast<FilterPlanNode*>(ch.get());
      t_ch->predicate_.Append(std::move(t_node->predicate_));
      return ch;
    } else if (t_node->ch_->type_ == PlanType::Project) {
      auto proj = std::move(t_node->ch_);
      auto t_proj = static_cast<ProjectPlanNode*>(proj.get());
      t_node->predicate_.ApplyExpr(
          t_proj->output_exprs_, t_proj->output_schema_);
      t_node->ch_ = std::move(t_proj->ch_);
      t_proj->ch_ = std::move(node);
      return proj;
    } else if (t_node->ch_->type_ == PlanType::Join) {
      auto A = static_cast<FilterPlanNode*>(node.get());
      auto B = static_cast<JoinPlanNode*>(A->ch_.get());
      B->predicate_.Append(std::move(A->predicate_));
      return std::move(A->ch_);
    } else if (t_node->ch_->type_ == PlanType::Aggregate) {
      auto agg = std::move(t_node->ch_);
      auto t_agg = static_cast<AggregatePlanNode*>(agg.get());
      t_node->predicate_.ApplyExpr(t_agg->output_exprs_, t_agg->output_schema_);
      t_agg->group_predicate_.Append(std::move(t_node->predicate_));
      return agg;
    } else if (t_node->ch_->type_ == PlanType::SeqScan) {
      auto seq = std::move(t_node->ch_);
      auto t_seq = static_cast<SeqScanPlanNode*>(seq.get());
      t_seq->predicate_.Append(std::move(t_node->predicate_));
      return seq;
    } else if (t_node->ch_->type_ == PlanType::RangeScan) {
      auto rseq = std::move(t_node->ch_);
      auto t_rseq = static_cast<RangeScanPlanNode*>(rseq.get());
      t_rseq->predicate_.Append(std::move(t_node->predicate_));
      return rseq;
    } else if (t_node->ch_->type_ == PlanType::HashJoin) {
      auto A = static_cast<FilterPlanNode*>(node.get());
      auto B = static_cast<HashJoinPlanNode*>(A->ch_.get());
      B->predicate_.Append(std::move(A->predicate_));
      return std::move(A->ch_);
    } 
    DB_ERR("Invalid node.");
  }
};
}  // namespace wing

#endif
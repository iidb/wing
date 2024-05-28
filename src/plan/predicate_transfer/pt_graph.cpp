#include "plan/predicate_transfer/pt_graph.hpp"

namespace wing {

PtGraph::PtGraph(const PlanNode* plan) { Dfs(plan); }

void PtGraph::Dfs(const PlanNode* plan) {
  if (!plan)
    return;
  if (plan->ch_) {
    Dfs(plan->ch_.get());
  }
  if (plan->ch2_) {
    Dfs(plan->ch2_.get());
  }
  if (plan->type_ == PlanType::Join || plan->type_ == PlanType::HashJoin) {
    auto& pred_vec =
        plan->type_ == PlanType::Join
            ? static_cast<const JoinPlanNode*>(plan)->predicate_
            : static_cast<const HashJoinPlanNode*>(plan)->predicate_;
    for (auto& expr : pred_vec.GetVec()) {
      auto L = expr.GetLeftTableName();
      auto R = expr.GetRightTableName();
      if (L && R && table_scan_plans_.count(L.value()) &&
          table_scan_plans_.count(R.value())) {
        graph_[L.value()].push_back(Edge(L.value(), R.value(),
            expr.GetLeftExpr()->clone(), expr.GetRightExpr()->clone()));
        graph_[R.value()].push_back(Edge(R.value(), L.value(),
            expr.GetRightExpr()->clone(), expr.GetLeftExpr()->clone()));
      }
    }
  }
  if (plan->type_ == PlanType::SeqScan || plan->type_ == PlanType::RangeScan) {
    auto table_name =
        plan->type_ == PlanType::SeqScan
            ? static_cast<const SeqScanPlanNode*>(plan)->table_name_in_sql_
            : static_cast<const RangeScanPlanNode*>(plan)->table_name_in_sql_;
    table_scan_plans_[table_name] = plan->clone();
    return;
  }
}

}  // namespace wing

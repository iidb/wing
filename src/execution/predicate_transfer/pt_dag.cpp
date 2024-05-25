#include "execution/predicate_transfer/pt_dag.hpp"

namespace wing {

PtGraph::PtGraph(const PlanNode* plan) { Dfs(plan); }

void PtGraph::Dfs(const PlanNode* plan) {
  if (!plan)
    return;
  if (plan->type_ == PlanType::Join) {
    auto join_table = static_cast<const JoinPlanNode*>(plan);
    for (auto& expr : join_table->predicate_.GetVec()) {
      auto L = expr.GetLeftTableName();
      auto R = expr.GetRightTableName();
      if (L && R) {
        graph_[L.value()].push_back(Edge(L.value(), R.value(),
            expr.GetLeftExpr()->clone(), expr.GetRightExpr()->clone()));
        graph_[R.value()].push_back(Edge(R.value(), L.value(),
            expr.GetRightExpr()->clone(), expr.GetLeftExpr()->clone()));
      }
    }
  }
  if (plan->ch_) {
    Dfs(plan->ch_.get());
  }
  if (plan->ch2_) {
    Dfs(plan->ch2_.get());
  }
}

}  // namespace wing

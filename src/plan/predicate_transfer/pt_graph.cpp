#include "plan/predicate_transfer/pt_graph.hpp"

namespace wing {

PtGraph::PtGraph(const PlanNode* plan) { Dfs(plan); }

void PtGraph::Dfs(const PlanNode* plan) {
}

}  // namespace wing

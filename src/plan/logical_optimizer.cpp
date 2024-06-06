#include "plan/expr_utils.hpp"
#include "plan/optimizer.hpp"
#include "plan/rules/push_down_filter.hpp"
#include "plan/rules/push_down_join_predicate.hpp"

namespace wing {

std::unique_ptr<PlanNode> LogicalOptimizer::Apply(
    std::unique_ptr<PlanNode> plan,
    const std::vector<std::unique_ptr<OptRule>>& rules) {
  while (true) {
    bool flag = false;
    for (auto& a : rules)
      if (a->Match(plan.get())) {
        plan = a->Transform(std::move(plan));
        flag = true;
      }
    if (!flag)
      break;
  }

  if (plan->ch_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules);
  }
  if (plan->ch2_ != nullptr) {
    plan->ch2_ = Apply(std::move(plan->ch2_), rules);
  }
  return plan;
}

std::unique_ptr<PlanNode> LogicalOptimizer::Optimize(
    std::unique_ptr<PlanNode> plan, DB& db) {
  std::vector<std::unique_ptr<OptRule>> R;
  R.push_back(std::make_unique<PushDownJoinPredicateRule>());
  R.push_back(std::make_unique<PushDownFilterRule>());
  plan = Apply(std::move(plan), R);

  return plan;
}

}  // namespace wing

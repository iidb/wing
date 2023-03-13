#include <queue>

#include "plan/optimizer.hpp"
#include "rules/convert_to_hash_join.hpp"

namespace wing {

std::unique_ptr<PlanNode> Apply(std::unique_ptr<PlanNode> plan, const std::vector<std::unique_ptr<OptRule>>& rules, const DB& db) {
  for (auto& a : rules) {
    if (a->Match(plan.get())) {
      plan = a->Transform(std::move(plan));
      break;
    }
  }
  if (plan->ch2_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules, db);
    plan->ch2_ = Apply(std::move(plan->ch2_), rules, db);
  } else if (plan->ch_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules, db);
  }
  return plan;
}

size_t GetTableNum(const PlanNode* plan) {
  if (plan->type_ == PlanType::Print || plan->type_ == PlanType::SeqScan) {
    return 1;
  }

  size_t ret = 0;
  if (plan->ch2_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
    ret += GetTableNum(plan->ch2_.get());
  } else if (plan->ch_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
  }
  return ret;
}

std::unique_ptr<PlanNode> CostBasedOptimizer::Optimize(std::unique_ptr<PlanNode> plan, const DB& db) {
  if (GetTableNum(plan.get()) < 6) {
    std::vector<std::unique_ptr<OptRule>> R;
    R.push_back(std::make_unique<ConvertToHashJoinRule>());
    plan = Apply(std::move(plan), R, db);  
    // TODO...
  } else {
    std::vector<std::unique_ptr<OptRule>> R;
    R.push_back(std::make_unique<ConvertToHashJoinRule>());
    plan = Apply(std::move(plan), R, db);  
  }
  return plan;
}

}  // namespace wing

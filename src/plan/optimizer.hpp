#ifndef SAKURA_OPTIMIZER_H__
#define SAKURA_OPTIMIZER_H__

#include "catalog/db.hpp"
#include "plan/plan.hpp"
#include "plan/rules/rule.hpp"

namespace wing {

class LogicalOptimizer {
 public:
  // Apply some rules to plan.
  static std::unique_ptr<PlanNode> Apply(std::unique_ptr<PlanNode> plan,
      const std::vector<std::unique_ptr<OptRule>>& rules);
  // Optimize the plan using logical rules.
  static std::unique_ptr<PlanNode> Optimize(
      std::unique_ptr<PlanNode> plan, DB& db);
};

class CostBasedOptimizer {
 public:
  // Optimize the plan using logical-to-physical rules and join reordering
  // rules.
  static std::unique_ptr<PlanNode> Optimize(
      std::unique_ptr<PlanNode> plan, DB& db);
};

}  // namespace wing

#endif
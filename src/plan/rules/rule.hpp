#ifndef SAKURA_RULE_H__
#define SAKURA_RULE_H__

#include "plan/plan.hpp"

namespace wing {
/**
 * The abstract class of rules.
 */
class OptRule {
 public:
  virtual ~OptRule() = default;
  // Check whether we can apply this rule.
  virtual bool Match(const PlanNode* node) = 0;
  // Transform the plan node.
  virtual std::unique_ptr<PlanNode> Transform(
      std::unique_ptr<PlanNode> node) = 0;
};

}  // namespace wing

#endif
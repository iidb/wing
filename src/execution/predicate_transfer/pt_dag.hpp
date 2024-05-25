#pragma once

#include <map>
#include <string>

#include "plan/plan.hpp"

namespace wing {

class PtGraph {
 public:
  struct Edge {
    std::string from;
    std::string to;
    std::unique_ptr<Expr> pred_from;
    std::unique_ptr<Expr> pred_to;

    Edge(const std::string& _from, const std::string& _to,
        std::unique_ptr<Expr> _pred_from, std::unique_ptr<Expr> _pred_to)
      : from(_from),
        to(_to),
        pred_from(std::move(_pred_from)),
        pred_to(std::move(_pred_to)) {}
  };

  const std::map<std::string, std::vector<Edge>>& Graph() const {
    return graph_;
  }

  PtGraph(const PlanNode* plan);

 private:
  void Dfs(const PlanNode* plan);

  std::map<std::string, std::vector<Edge>> graph_;
};

}  // namespace wing

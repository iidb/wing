#include "execution/predicate_transfer/pt_reducer.hpp"

namespace wing {

void PtReducer::Execute(const PtGraph& dag) {
  std::vector<std::string> order;
  std::map<std::string, int> mp;

  std::vector<std::string> Q;

  auto& G = dag.Graph();
  if (!G.size()) {
    return;
  }

  Q.push_back(G.begin()->first);
  mp[Q.front()] = order.size();
  order.push_back(Q.front());

  while (Q.size()) {
    auto table = Q.front();
    Q.pop_front();
    for (auto& edge : G[table]) {
      if (!mp.count(edge.to)) {
        mp[edge.to] = order.size();
        order.push_back(edge.to);
        Q.push_back(edge.to);
      }
    }
  }

  for (auto table : order) {
    result_predicates_[table] = InitBitVector(table);
  }

  for (int i = order.size() - 1; i >= 0; --i) {
    auto table = order[i];
    for (auto& edge : G[table])
      if (mp[edge.to] >= i) {
        PredicateTransfer(edge.to, table, edge.pred_to, edge.pred_from);
      }
  }

  for (int i = 0; i < order.size(); i++) {
    auto table = order[i];
    for (auto& edge : G[table])
      if (mp[edge.to] < i) {
        PredicateTransfer(edge.to, table, edge.pred_to, edge.pred_from);
      }
  }
}

void PtReducer::PredicateTransfer(std::string from, std::string to,
    const Expr* from_expr, const Expr* to_expr) {}

BitVector PtReducer::InitBitVector(std::string table) {}

}  // namespace wing

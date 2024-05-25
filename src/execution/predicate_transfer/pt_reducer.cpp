#include "execution/predicate_transfer/pt_reducer.hpp"

#include "common/bloomfilter.hpp"
#include "execution/executor.hpp"
#include "execution/predicate_transfer/vb_vupdater.hpp"

namespace wing {

void PtReducer::Execute() {
  std::vector<std::string> order;
  std::map<std::string, int> mp;

  std::vector<std::string> Q;

  auto& G = graph_->Graph();
  if (G.size() <= 1) {
    return;
  }

  Q.push_back(G.begin()->first);
  mp[Q.front()] = order.size();
  order.push_back(Q.front());

  while (Q.size()) {
    auto table = Q.front();
    Q.erase(Q.begin());
    for (auto& edge : G.at(table)) {
      if (!mp.count(edge.to)) {
        mp[edge.to] = order.size();
        order.push_back(edge.to);
        Q.push_back(edge.to);
      }
    }
  }

  for (auto& [name, plan] : graph_->GetTableScanPlans()) {
    result_bv_[name] =
        plan->type_ == PlanType::SeqScan
            ? static_cast<SeqScanPlanNode*>(plan.get())->valid_bits_
            : static_cast<RangeScanPlanNode*>(plan.get())->valid_bits_;
  }

  for (int i = order.size() - 1; i >= 0; --i) {
    auto table = order[i];
    for (auto& edge : G.at(table))
      if (mp[edge.to] >= i) {
        PredicateTransfer(
            edge.to, table, edge.pred_to.get(), edge.pred_from.get());
      }
  }

  for (int i = 0; i < order.size(); i++) {
    auto table = order[i];
    for (auto& edge : G.at(table))
      if (mp[edge.to] < i) {
        PredicateTransfer(
            edge.to, table, edge.pred_to.get(), edge.pred_from.get());
      }
  }
}

void PtReducer::PredicateTransfer(std::string from, std::string to,
    const Expr* from_expr, const Expr* to_expr) {
  /**
   * Step 1. Construct two plans:
   *
   * SeqScanNode (from) -> ProjectPlanNode
   *
   * SeqScanNode (to) -> ProjectPlanNode.
   *
   */
  auto plan_from = std::make_unique<ProjectPlanNode>();
  {
    plan_from->output_exprs_.push_back(from_expr->clone());
    plan_from->output_schema_.Append(
        OutputColumnData{0, "", "a", from_expr->ret_type_, 0});
    auto from_seq_plan = graph_->GetTableScanPlans().at(from)->clone();
    plan_from->ch_ = std::move(from_seq_plan);
  }
  auto plan_to = std::make_unique<ProjectPlanNode>();
  {
    plan_to->output_exprs_.push_back(to_expr->clone());
    plan_to->output_schema_.Append(
        OutputColumnData{0, "", "a", to_expr->ret_type_, 0});
    auto to_seq_plan = graph_->GetTableScanPlans().at(to)->clone();
    plan_to->ch_ = std::move(to_seq_plan);
  }
  /**
   * Step 2. Construct executors
   *
   */
  auto exe_from = ExecutorGenerator::GenerateVec(plan_from.get(), db_, txn_id_);
  auto exe_to = ExecutorGenerator::GenerateVec(plan_to.get(), db_, txn_id_);
  /**
   * Step3. Execute exe_from to get bloom filter,
   *
   */
  BfVecCreator bf_creator(n_bits_per_key_, std::move(exe_from));
  bf_creator.Execute();
  auto& bloom_filter = bf_creator.GetResult();
  /**
   * Step4. Execute exe_to and update valid bits of table `to`.
   *
   */
  VbVecUpdater updater(std::move(exe_to));
  if (!result_bv_[to]) {
    DB_ERR("pointer to result bitvector cannot be nullptr.");
  }
  updater.Execute(bloom_filter, *result_bv_[to]);
}

}  // namespace wing

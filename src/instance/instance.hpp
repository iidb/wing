#pragma once

#include <future>
#include <memory>
#include <string>

#include "catalog/options.hpp"
#include "instance/resultset.hpp"
#include "plan/plan.hpp"
#include "transaction/txn.hpp"
#include "transaction/txn_manager.hpp"

namespace wing {

class Instance {
 public:
  Instance(std::string_view db_file, WingOptions options);
  ~Instance();
  ResultSet Execute(std::string_view statement);
  ResultSet Execute(std::string_view statement, txn_id_t txn_id);
  void ExecuteShell();
  void Analyze(std::string_view table_name);
  TxnManager& GetTxnManager();

  // Give a SQL statement, return the optimized plan.
  // Used for testing optimizer.
  // (It can also used to avoid parsing... But it's not useful in this course.)
  std::unique_ptr<PlanNode> GetPlan(std::string_view statement);

  // print plan for every statement except for metadata statements such as
  // create/drop table.
  void SetDebugPrintPlan(bool value);

  // Enable predicate transfer or not.
  void SetEnablePredTrans(bool value);

  // Set true cardinality hints
  void SetTrueCardinalityHints(
      const std::vector<std::pair<std::vector<std::string>, double>>& cards);

  // Enable cost based optimizer or not.
  // If cost based optimizer is not enabled, there will only be rule based
  // optimizer.
  void SetEnableCostBased(bool value);

 private:
  class Impl;
  std::unique_ptr<Impl> ptr_;
  void Analyze(std::string_view table_name, txn_id_t txn_id);
};

}  // namespace wing

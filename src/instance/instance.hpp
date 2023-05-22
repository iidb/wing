#ifndef SAKURA_INSTANCE_H__
#define SAKURA_INSTANCE_H__

#include <future>
#include <memory>
#include <string>

#include "instance/resultset.hpp"
#include "plan/plan.hpp"
#include "transaction/txn.hpp"
#include "transaction/txn_manager.hpp"

namespace wing {

class Instance {
 public:
  Instance(std::string_view db_file, bool use_jit_flag);
  ~Instance();
  ResultSet Execute(std::string_view statement);
  ResultSet Execute(std::string_view statement, txn_id_t txn_id);
  void ExecuteShell();
  void Analyze(std::string_view table_name);
  TxnManager &GetTxnManager();

  // Give a SQL statement, return the optimized plan.
  // Used for testing optimizer.
  // (It can also used to avoid parsing... But it's not useful in this course.)
  std::unique_ptr<PlanNode> GetPlan(std::string_view statement);

 private:
  class Impl;
  std::unique_ptr<Impl> ptr_;
  void Analyze(std::string_view table_name, txn_id_t txn_id);
};

}  // namespace wing

#endif
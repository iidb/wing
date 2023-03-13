#ifndef SAKURA_EXECUTOR_H__
#define SAKURA_EXECUTOR_H__

#include <numeric>

#include "plan/plan.hpp"
#include "execution/exprdata.hpp"
#include "catalog/db.hpp"
#include "parser/expr.hpp"
#include "storage/storage.hpp"

namespace wing {

/**
 * Init(): Only allocate memory and set some flags, don't evaluate expressions or read/write tuples.
 * Next(): Do operations for each tuple. Return invalid result if it has completed.
 * Clear(): Release the unused memory. E.g. JoinExecutor after returning results.
 *
 * The first Next() returns the first tuple. The i-th Next() returns the i-th tuple.
 * It is illegal to invoke Next() after Next() returns invalid result.
 * Ensure that Init is invoked only once before executing.
 *
 * You should ensure that the InputTuplePtr is valid until Next() is invoked again.
 */
class Executor {
 public:
  virtual ~Executor() = default;
  virtual void Init() = 0;
  virtual InputTuplePtr Next() = 0;
};

class ExecutorGenerator {
 public:
  static std::unique_ptr<Executor> Generate(const PlanNode* plan, DB& db, size_t txn_id);

 private:
};

}  // namespace wing

#endif
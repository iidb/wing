#pragma once

#include <numeric>

#include "catalog/db.hpp"
#include "execution/volcano/expr_executor.hpp"
#include "parser/expr.hpp"
#include "plan/plan.hpp"
#include "storage/storage.hpp"
#include "transaction/txn.hpp"
#include "type/tuple_batch.hpp"

namespace wing {

/**
 * Init(): Only allocate memory and set some flags, don't evaluate expressions
 * or read/write tuples. Next(): Do operations for each tuple. Return invalid
 * result if it has completed.
 *
 * The first Next() returns the first tuple. The i-th Next() returns the i-th
 * tuple. It is illegal to invoke Next() after Next() returns invalid result.
 * Ensure that Init is invoked only once before executing.
 *
 * You should ensure that the SingleTuple is valid until Next() is invoked
 * again.
 */
class Executor {
 public:
  virtual ~Executor() = default;
  virtual void Init() = 0;
  virtual SingleTuple Next() = 0;
};

class VecExecutor {
 public:
  virtual ~VecExecutor() = default;
  virtual void Init() = 0;
  virtual TupleBatch Next() = 0;
};

#ifdef BUILD_JIT
class JitExecutorGenerator {
 public:
  static std::unique_ptr<Executor> Generate(
      const PlanNode* plan, DB& db, size_t txn_id);

 private:
};
#else
class JitExecutorGenerator {
 public:
  static std::unique_ptr<Executor> Generate(const PlanNode*, DB&, size_t) {
    return nullptr;
  }

 private:
};
#endif

class ExecutorGenerator {
 public:
  static std::unique_ptr<Executor> Generate(
      const PlanNode* plan, DB& db, txn_id_t txn_id);
  static std::unique_ptr<Executor> GenerateVec(
      const PlanNode* plan, DB& db, txn_id_t txn_id);

 private:
};

}  // namespace wing

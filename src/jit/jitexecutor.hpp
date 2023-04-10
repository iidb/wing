#ifndef SAKURA_JIT_EXECUTOR_H__
#define SAKURA_JIT_EXECUTOR_H__

#include <memory>

#include "catalog/db.hpp"
#include "catalog/schema.hpp"
#include "execution/executor.hpp"

namespace wing {

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
}  // namespace wing

#endif
#include "execution/predicate_transfer/pt_reducer.hpp"

#include "common/bloomfilter.hpp"
#include "execution/executor.hpp"
#include "execution/predicate_transfer/pt_vcreator.hpp"
#include "execution/predicate_transfer/pt_vupdater.hpp"

namespace wing {

void PtReducer::Execute() { DB_ERR("Not implemented!"); }

void PtReducer::PredicateTransfer(std::string from, std::string to,
    const Expr* from_expr, const Expr* to_expr) {
  DB_ERR("Not implemented!");
}

}  // namespace wing

#include "execution/predicate_transfer/pt_reducer.hpp"

#include "common/bloomfilter.hpp"
#include "execution/executor.hpp"
#include "execution/predicate_transfer/pt_vcreator.hpp"
#include "execution/predicate_transfer/pt_vupdater.hpp"

namespace wing {

void PtReducer::Execute() { DB_ERR("Not implemented!"); }

std::vector<std::string> PtReducer::GenerateFilter(
    const std::string& table, const std::vector<const Expr*>& exprs) {
  DB_ERR("Not implemented!");
}

void PtReducer::PredicateTransfer(const std::string& table,
    const std::vector<const Expr*>& exprs,
    const std::vector<std::string>& filters) {
  DB_ERR("Not implemented!");
}

}  // namespace wing

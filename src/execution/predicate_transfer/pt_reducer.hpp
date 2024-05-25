#pragma once

#include "execution/predicate_transfer/bf_vcreator.hpp"
#include "execution/predicate_transfer/pt_graph.hpp"

namespace wing {

class PtReducer {
 public:
  PtReducer(DB& db, size_t txn_id, size_t n_bits_per_key,
      std::shared_ptr<PtGraph> graph)
    : db_(db),
      txn_id_(txn_id),
      n_bits_per_key_(n_bits_per_key),
      graph_(graph) {}

  void Execute();

  const std::map<std::string, std::shared_ptr<BitVector>>& GetResultBitVector()
      const {
    return result_bv_;
  }

 private:
  void PredicateTransfer(std::string from, std::string to,
      const Expr* from_expr, const Expr* to_expr);

  /* Mappings from table name to bit vector. */
  std::map<std::string, std::shared_ptr<BitVector>> result_bv_;

  DB& db_;
  size_t txn_id_;
  size_t n_bits_per_key_;
  std::shared_ptr<PtGraph> graph_;
};

}  // namespace wing

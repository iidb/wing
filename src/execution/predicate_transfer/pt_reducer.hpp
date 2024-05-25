#pragma once

#include "execution/predicate_transfer/pt_creator.hpp"
#include "execution/predicate_transfer/pt_dag.hpp"

namespace wing {

class PtReducer {
 public:
  PtReducer(DB& db) : db_(db) {}

  void Execute(const PtGraph& dag);

 private:
  void PredicateTransfer(std::string from, std::string to,
      const Expr* from_expr, const Expr* to_expr);

  BitVector InitBitVector(std::string table);

  /* Mappings from table name to bit vector. */
  std::map<std::string, BitVector> result_predicates_;

  DB& db_;
};

}  // namespace wing

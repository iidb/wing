#ifndef SAKURA_CARD_EST_H__
#define SAKURA_CARD_EST_H__

#include <string>
#include "catalog/db.hpp"
#include "plan/plan_expr.hpp"
#include "plan/output_schema.hpp"

namespace wing {

class CardEstimator {
 public:
  
  // Necessary data for a group of tables. 
  class Summary {
    public:
      double size_{0};
      std::vector<std::pair<int, double>> distinct_rate_;
  };

  // Use DB statistics to estimate the size of the output of seq scan.
  // We assume that columns are uniformly distributed and independent.
  // You should consider predicates which contain two operands and one is a constant.
  // There are some cases:
  // (1) A.a = 1; 1 = A.a; Use CountMinSketch.
  // (2) A.a > 1; 1 > A.a; or A.a <= 1; Use the maximum element and the minimum element of the table.
  // (3) You should ignore other predicates, such as A.a * 2 + A.b < 1000 and A.a < A.b.
  // (4) 1 > 2; Return 0. You can ignore it, because it should be filtered before optimization.
  // You should check the type of each column and write codes for each case, unfortunately.
  static Summary EstimateTable(std::string_view table_name, const PredicateVec& predicates, const OutputSchema& schema, DB& db) {
    Summary ret;
    return ret;
  }

  // Only consider about equality predicates such as 'A.a = B.b'
  // For other join predicates, you should ignore them.
  static Summary EstimateJoinEq(
    const PredicateVec& predicates, 
    const Summary& build, 
    const Summary& probe
  ) {
    Summary ret;
    return ret;
  }
};

}

#endif
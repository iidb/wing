#ifndef SAKURA_Plan_H__
#define SAKURA_Plan_H__

#include <string>
#include <vector>

#include "catalog/schema.hpp"
#include "common/bitvector.hpp"
#include "parser/ast.hpp"
#include "parser/expr.hpp"
#include "plan/output_schema.hpp"
#include "plan/plan_expr.hpp"
#include "type/vector.hpp"

namespace wing {

enum class PlanType {
  Project = 0,
  SeqScan,
  Filter,
  Join,
  Aggregate,
  Order,
  Limit,
  Insert,
  Delete,
  Update,
  Print,
  Distinct,
  // Different implementations for some operators.
  HashJoin,
  MergeSortJoin,
  RangeScan,
};

/**
 * PlanNode: It is used to represent a relational expression.
 *
 *              +-- ProjectPlanNode: It computes a set of "select expressions"
 * from its input relational expression.
 *              |
 * PlanNode   --+-- SeqScanPlanNode: It returns the contents of a table.
 *              |
 *              +-- FilterPlanNode: It iterates over its input and returns
 * elements for which condition evaluates to true. |                    The
 * predicate doesn't contain aggregate functions. | Predicates containing
 * aggregate functions are stored in AggregatePlanNode.
 *              |
 *              +-- JoinPlanNode: Each output row has columns from the left and
 * right inputs. |                  The set of output rows is a subset of the
 * cartesian product of the two inputs. |                  It iterates the set
 * and returns the rows which satisfy some conditions. |                  It may
 * not be implemented by directly iterates the cross product, for example, use
 * hash join.
 *              |
 *              +-- AggregatePlanNode: It eliminates duplicates and computes
 * totals.
 *              |
 *              +-- OrderByPlanNode: It sorts the input rows by some fields.
 *              |
 *              +-- LimitPlanNode: It outputs a part of input rows.
 *              |
 *              +-- InsertPlanNode: Insert the input rows to a table.
 *              |
 *              +-- UpdatePlanNode: Get the primary key from input rows and
 * update the rows.
 *              |
 *              +-- DeletePlanNode: Get the primary key from input rows and
 * delete the input rows.
 *              |
 *              +-- PrintPlanNode: It directly print rows.
 *              |
 *              +-- DistinctPlanNode: It eliminate duplicate rows.
 */
class PlanNode {
 public:
  PlanNode(PlanType type) : type_(type) {}
  virtual ~PlanNode() = default;
  virtual std::string ToString() const { return "Unknown Logical PlanNode"; }
  virtual std::unique_ptr<PlanNode> clone() const = 0;
  PlanType type_;
  OutputSchema output_schema_;
  BitVector table_bitset_;
  std::unique_ptr<PlanNode> ch_{nullptr};
  std::unique_ptr<PlanNode> ch2_{nullptr};
};

class ProjectPlanNode : public PlanNode {
 public:
  ProjectPlanNode() : PlanNode(PlanType::Project) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  std::vector<std::unique_ptr<Expr>> output_exprs_;
};

class SeqScanPlanNode : public PlanNode {
 public:
  SeqScanPlanNode() : PlanNode(PlanType::SeqScan) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  std::string table_name_;
  PredicateVec predicate_;
};

class FilterPlanNode : public PlanNode {
 public:
  FilterPlanNode() : PlanNode(PlanType::Filter) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  PredicateVec predicate_;
};

class JoinPlanNode : public PlanNode {
 public:
  JoinPlanNode() : PlanNode(PlanType::Join) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  PredicateVec predicate_;
};

class AggregatePlanNode : public PlanNode {
 public:
  AggregatePlanNode() : PlanNode(PlanType::Aggregate) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  // The predicates in HAVING clause.
  // For example, select * from A where A.a > 1 group by A.b having sum(A.c)
  // > 10. group_predicate_ = [sum(A.c)]
  PredicateVec group_predicate_;
  std::vector<std::unique_ptr<Expr>> output_exprs_;
  std::vector<std::unique_ptr<Expr>> group_by_exprs_;
};

class OrderByPlanNode : public PlanNode {
 public:
  OrderByPlanNode() : PlanNode(PlanType::Order) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  // std::pair<RetType, bool>
  // The first field is the type of the expression for sorting.
  // The second field denotes the direction, i.e. asc or desc. True is asc.
  std::vector<std::pair<RetType, bool>> order_by_exprs_;
  size_t order_by_offset_;
};

class LimitPlanNode : public PlanNode {
 public:
  LimitPlanNode() : PlanNode(PlanType::Limit) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  // limit_size_, offset_ denotes the output interval.
  size_t limit_size_{0}, offset_{0};
};

class InsertPlanNode : public PlanNode {
 public:
  InsertPlanNode() : PlanNode(PlanType::Insert) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  std::string table_name_;
};

class DeletePlanNode : public PlanNode {
 public:
  DeletePlanNode() : PlanNode(PlanType::Delete) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  std::string table_name_;
};

class UpdatePlanNode : public PlanNode {
 public:
  UpdatePlanNode() : PlanNode(PlanType::Update) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  std::string table_name_;
  // std::pair<uint32_t, std::unique_ptr<Expr>>
  // The first field is the column index.
  // The second field is the expression.
  std::vector<std::pair<uint32_t, std::unique_ptr<Expr>>> updates_;
};

class PrintPlanNode : public PlanNode {
 public:
  PrintPlanNode() : PlanNode(PlanType::Print) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  std::shared_ptr<StaticFieldVector> values_;
  // The number of fields in a tuple.
  size_t num_fields_per_tuple_{0};
};

class DistinctPlanNode : public PlanNode {
 public:
  DistinctPlanNode() : PlanNode(PlanType::Distinct) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
};

class HashJoinPlanNode : public PlanNode {
 public:
  HashJoinPlanNode() : PlanNode(PlanType::HashJoin) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  // left_hash_exprs are hash keys of left table (build table)
  // right_hash_exprs are hash keys of right table
  // left_hash_exprs[i] is corresponding to right_hash_exprs_[i]
  std::vector<std::unique_ptr<Expr>> left_hash_exprs_;
  std::vector<std::unique_ptr<Expr>> right_hash_exprs_;
  PredicateVec predicate_;
};

class MergeSortJoinPlanNode : public PlanNode {
 public:
  MergeSortJoinPlanNode() : PlanNode(PlanType::MergeSortJoin) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  std::vector<std::unique_ptr<Expr>> merge_keys_;
  PredicateVec predicate_;
};

class RangeScanPlanNode : public PlanNode {
 public:
  RangeScanPlanNode() : PlanNode(PlanType::RangeScan) {}
  std::string ToString() const override;
  std::unique_ptr<PlanNode> clone() const override;
  std::string table_name_;
  /* Field is the key */
  /* The boolean represents whether the endpoint of the interval is closed.*/
  std::pair<Field, bool> range_l_;
  std::pair<Field, bool> range_r_;
  PredicateVec predicate_;
};

// This is used to generate a base plan after generating AST.
class BasicPlanGenerator {
 public:
  BasicPlanGenerator(const DBSchema& schema);
  ~BasicPlanGenerator();
  std::pair<std::unique_ptr<PlanNode>, std::string> Plan(Statement* statement);

 private:
  class Impl;
  std::unique_ptr<Impl> ptr_;
};

}  // namespace wing

#endif
#ifndef SAKURA_EXPR_H__
#define SAKURA_EXPR_H__

#include <functional>
#include <memory>
#include <vector>

#include "type/field_type.hpp"

namespace wing {

/**
 * Expressions.
 *
 *         +- BinaryConditionExpr: Two child exprs and AND (and) or OR (or),
 *         |                       or LEQ (<=) or REQ (>=) or EQ (=) or NEQ (<>) 
 *         |                       Children exprs of types FLOAT or STRING is
 *         |                       not allowed for all operands. 
 *         |                       Return type is INT.
 *         |
 * Expr  --+- BinaryExpr: Two child exprs and ADD (+) or SUB (-) or MUL (*) or
 * DIV (/) or MOD (%) or BITAND (&) or BITXOR (^) or BITOR (|) 
 *         | BITLSH (<<) or BITRSH (>>) or LT (<) or RT (>) 
 *         | 
 *         | Child exprs of types STRING is not allowed for all operands. 
 *         | Child exprs of types FLOAT is not allowed for 
 *         | bitwise operands and MOD, 
 *         | i.e. BITLSH, BITRSH, BITAND, BITXOR, BITOR, MOD 
 *         | Return type is the maximum return type within the two child exprs.
 *         |
 *         +- UnaryExpr: NEG (-) or NOT (not) and a child Expr.
 *         |             Child expr of type STRING is not allowed.
 *         |             Child expr of type FLOAT is not allow for operand NOT.
 *         |             Retun type is the return type of the child expr.
 *         |
 *         |
 *         +- LiteralStringExpr: A string constant. Return type is STRING.
 *         |
 *         +- LiteralIntegerExpr: An integer constant. Return type is INT.
 *         |
 *         +- LiteralFloatExpr: A real number constant. Return type is FLOAT.
 *         |
 *         +- ColumnExpr: ID that denotes a column from table. Return type is
 * the type of the column.
 *         |
 *         +- CastExpr: Cast an expression of returning type INT to FLOAT or
 * vice versa.
 *         |
 *         +- AggregateFunctionExpr: An aggregate function.
 */

enum class OpType {
  ADD = 0,
  SUB,
  MUL,
  DIV,
  MOD,
  BITAND,
  BITXOR,
  BITOR,
  BITLSH,
  BITRSH,
  LT,
  GT,
  LEQ,
  GEQ,
  EQ,
  NEQ,
  AND,
  OR,
  NOT,
  NEG
};

/** 
 * RetType can be converted to FieldType:
 *  RetType::INT - FieldType::INT64
 *  RetType::FLOAT - FieldType::FLOAT64
 *  RetType::STRING - FieldType::VARCHAR
 */
enum class RetType { INT = 0, FLOAT, STRING };

enum class ExprType {
  LITERAL_STRING = 0, // LiteralStringExpr
  LITERAL_INTEGER,    // LiteralIntegerExpr
  LITERAL_FLOAT,      // LiteralFloatExpr
  BINOP,              // BinaryExpr
  BINCONDOP,          // BinaryConditionExpr
  UNARYOP,            // UnaryExpr
  UNARYCONDOP,        // UnaryConditionExpr
  COLUMN,             // ColumnExpr
  CAST,               // CastExpr
  AGGR,               // AggregateFunctionExpr
};

struct Expr {
  ExprType type_;
  RetType ret_type_;
  std::unique_ptr<Expr> ch0_, ch1_;
  Expr(ExprType type) : type_(type) {}
  Expr(ExprType type, std::unique_ptr<Expr>&& ch0)
    : type_(type), ch0_(std::move(ch0)) {}
  Expr(ExprType type, std::unique_ptr<Expr>&& ch0, std::unique_ptr<Expr>&& ch1)
    : type_(type), ch0_(std::move(ch0)), ch1_(std::move(ch1)) {}
  virtual ~Expr() = default;
  /* Print the expr. */
  virtual std::string ToString() const = 0;
  /* Clone an expr. */
  virtual std::unique_ptr<Expr> clone() const = 0;
};

struct BinaryExpr : public Expr {
  OpType op_;
  BinaryExpr(
      OpType op, std::unique_ptr<Expr>&& ch0, std::unique_ptr<Expr>&& ch1)
    : Expr(ExprType::BINOP, std::move(ch0), std::move(ch1)), op_(op) {}
  std::string ToString() const override;
  std::unique_ptr<Expr> clone() const override;
};

struct BinaryConditionExpr : public Expr {
  OpType op_;
  BinaryConditionExpr(
      OpType op, std::unique_ptr<Expr>&& ch0, std::unique_ptr<Expr>&& ch1)
    : Expr(ExprType::BINCONDOP, std::move(ch0), std::move(ch1)), op_(op) {}
  std::string ToString() const override;
  std::unique_ptr<Expr> clone() const override;
};

struct UnaryExpr : public Expr {
  OpType op_;
  UnaryExpr(OpType op, std::unique_ptr<Expr>&& ch)
    : Expr(ExprType::UNARYOP, std::move(ch)), op_(op) {}
  std::string ToString() const override;
  std::unique_ptr<Expr> clone() const override;
};

struct UnaryConditionExpr : public Expr {
  OpType op_;
  UnaryConditionExpr(OpType op, std::unique_ptr<Expr>&& ch)
    : Expr(ExprType::UNARYCONDOP, std::move(ch)), op_(op) {}
  std::string ToString() const override;
  std::unique_ptr<Expr> clone() const override;
};

struct LiteralStringExpr : public Expr {
  std::string literal_value_;
  LiteralStringExpr(std::string_view literal_value)
    : Expr(ExprType::LITERAL_STRING), literal_value_(literal_value) {}
  std::string ToString() const override;
  std::unique_ptr<Expr> clone() const override;
};

struct LiteralIntegerExpr : public Expr {
  int64_t literal_value_;
  LiteralIntegerExpr(int64_t _literal_value)
    : Expr(ExprType::LITERAL_INTEGER), literal_value_(_literal_value) {}
  std::string ToString() const override;
  std::unique_ptr<Expr> clone() const override;
};

struct LiteralFloatExpr : public Expr {
  double literal_value_;
  LiteralFloatExpr(double _literal_value)
    : Expr(ExprType::LITERAL_FLOAT), literal_value_(_literal_value) {}
  std::string ToString() const override;
  std::unique_ptr<Expr> clone() const override;
};

struct ColumnExpr : public Expr {
  std::string table_name_;
  std::string column_name_;
  /* Unique id. */
  uint32_t id_in_column_name_table_;
  /* The id of table. */
  uint32_t id_table_in_planner_;
  ColumnExpr(std::string table_name, std::string column_name)
    : Expr(ExprType::COLUMN),
      table_name_(table_name),
      column_name_(column_name) {}
  std::string ToString() const override;
  std::unique_ptr<Expr> clone() const override;
};

struct CastExpr : public Expr {
  CastExpr(std::unique_ptr<Expr>&& ch) : Expr(ExprType::CAST, std::move(ch)) {}
  std::string ToString() const override;
  std::unique_ptr<Expr> clone() const override;
};

struct AggregateFunctionExpr : public Expr {
  std::string func_name_;
  AggregateFunctionExpr(std::string_view func_name, std::unique_ptr<Expr>&& ch)
    : Expr(ExprType::AGGR, std::move(ch)), func_name_(func_name) {}
  std::string ToString() const override;
  std::unique_ptr<Expr> clone() const override;
};

}  // namespace wing

#endif

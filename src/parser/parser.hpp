#ifndef SAKURA_PARSER_H__
#define SAKURA_PARSER_H__

#include <memory>
#include <optional>
#include <string>

#include "catalog/schema.hpp"
#include "parser/ast.hpp"
#include "plan/plan.hpp"

namespace wing {

class ParserResult {
 public:
  ParserResult(std::unique_ptr<Statement>&& statement, std::unique_ptr<PlanNode> plan, std::string&& err_msg)
      : statement_(std::move(statement)), plan_(std::move(plan)), err_msg_(std::move(err_msg)) {}

  ParserResult(std::string&& err_msg) : err_msg_(std::move(err_msg)) {}

  bool Valid() { return err_msg_ == ""; }

  std::string GetErrorMsg() { return err_msg_; }

  const std::unique_ptr<Statement>& GetAST() const { return statement_; }

  const std::unique_ptr<PlanNode>& GetPlan() const { return plan_; }

  void Clear() {
    statement_ = nullptr;
    plan_ = nullptr;
    err_msg_ = "";
  }

 private:
  std::unique_ptr<Statement> statement_;
  std::unique_ptr<PlanNode> plan_;
  std::string err_msg_;
};

class Parser {
 public:
  Parser();

  ~Parser();

  ParserResult Parse(std::string_view statement, const DBSchema& db_schema);

 private:
  class Impl;
  std::unique_ptr<Impl> ptr_;
};

}  // namespace wing

#endif
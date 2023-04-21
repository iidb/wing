#include "plan/plan.hpp"

#include <numeric>

#include "execution/exprdata.hpp"
#include "type/field.hpp"

namespace wing {

class BasicPlanGenerator::Impl {
  class PlannerException : public std::exception {
    std::string errmsg_;

   public:
    PlannerException(std::string errmsg) : errmsg_(errmsg) {}
    const char* what() const noexcept { return errmsg_.c_str(); }
  };

  class AnalysisExprResult {
   public:
    bool is_constant_{false};
    uint32_t aggregate_expr_num{0};
  };

 public:
  Impl(const DBSchema& schema) : schema_(schema) {}

  std::pair<std::unique_ptr<PlanNode>, std::string> Plan(Statement* statement) {
    try {
      if (statement->type_ == StatementType::SELECT) {
        return {plan_select(static_cast<SelectStatement*>(statement)), ""};
      } else if (statement->type_ == StatementType::INSERT) {
        return {plan_insert(static_cast<InsertStatement*>(statement)), ""};
      } else if (statement->type_ == StatementType::UPDATE) {
        return {plan_update(static_cast<UpdateStatement*>(statement)), ""};
      } else if (statement->type_ == StatementType::DELETE) {
        return {plan_delete(static_cast<DeleteStatement*>(statement)), ""};
      } else {
        throw PlannerException("Unrecognized statement type");
      }
    } catch (const PlannerException& e) {
      return {nullptr, fmt::format("Planner error message: {}", e.what())};
    }
    return {nullptr, "planner error"};
  }

 private:
  // get_value_id get the index of (table_name, column_name) in
  // column_name_table column_name_table_index is set of indexes that are valid
  // in current scope. For example, in the following SQL statement:
  //      select a, c from (select a as b from A) as D, (select b as c from A)
  //      as D
  // column name 'b' is valid in both subqueries, i.e., b in the second subquery
  // doesn't refer to b in the first subquery.
  uint32_t get_value_id(const auto& table_name, const auto& column_name,
      const OutputSchema& column_name_table) {
    uint32_t ret = ~0u;
    for (uint32_t index = 0;
         const auto& temp_column : column_name_table.GetCols()) {
      const auto& tab = temp_column.table_name_;
      const auto& col = temp_column.column_name_;
      if ((tab == table_name || table_name == "") && column_name == col) {
        if (~ret) {
          if (table_name != "")
            throw PlannerException(fmt::format(
                "Column \'{}.{}\' is ambigorous.", table_name, column_name));
          else
            throw PlannerException(fmt::format(
                "Column \'{}\' is ambigorous: \'{}.{}\', \'{}.{}\'.",
                column_name, column_name_table[ret].table_name_, column_name,
                tab, column_name));
        }
        // DB_INFO("{}, {}", tab, col.name_);
        ret = index;
      }
      index += 1;
    }
    if (ret == (~0u)) {
      if (table_name != "")
        throw PlannerException(fmt::format(
            "Column \'{}.{}\' is undefined.", table_name, column_name));
      else
        throw PlannerException(
            fmt::format("Column \'{}\' is undefined.", column_name));
    }
    return ret;
  }
  // analysis_expr analyses the return type and find the index of (table_name,
  // column_name) using column_name_table and column_name_table_index It also
  // calculates the number of aggregation functions. It checks: (1) whether this
  // is a constant expression. (2) whether it has nested aggregation functions.
  // (3) whether there exists invalid usage of operands.
  // If the types of operands are different, it will try to cast one of them to
  // higher type. For example, if the expression is 1 (INT) + 1.1 (FLOAT). Then
  // it will cast 1 (INT) to FLOAT and add a CastExpr to the Expr.
  AnalysisExprResult analysis_expr(
      Expr* expr, const OutputSchema& column_name_table) {
    AnalysisExprResult result;
    if (expr->type_ == ExprType::LITERAL_FLOAT) {
      expr->ret_type_ = RetType::FLOAT;
      result.is_constant_ = true;
    } else if (expr->type_ == ExprType::LITERAL_STRING) {
      expr->ret_type_ = RetType::STRING;
      result.is_constant_ = true;
    } else if (expr->type_ == ExprType::LITERAL_INTEGER) {
      expr->ret_type_ = RetType::INT;
      result.is_constant_ = true;
    } else if (expr->type_ == ExprType::CAST) {
      DB_ERR("Internal Error: Expr before analysis should not have CastExpr.");
    } else if (expr->type_ == ExprType::BINOP) {
      auto this_expr = static_cast<BinaryExpr*>(expr);
      auto retl = analysis_expr(this_expr->ch0_.get(), column_name_table);
      auto retr = analysis_expr(this_expr->ch1_.get(), column_name_table);
      result.is_constant_ = retl.is_constant_ && retr.is_constant_;
      result.aggregate_expr_num =
          retl.aggregate_expr_num + retr.aggregate_expr_num;
      auto ltype = this_expr->ch0_->ret_type_;
      auto rtype = this_expr->ch1_->ret_type_;
      if (ltype != rtype) {
        if (ltype == RetType::STRING || rtype == RetType::STRING)
          throw PlannerException(
              "Arithmetic operators between STRINGs and other type are "
              "invalid.");
        if (ltype == RetType::FLOAT) {
          this_expr->ch1_ =
              std::make_unique<CastExpr>(std::move(this_expr->ch1_));
          this_expr->ch1_->ret_type_ = RetType::FLOAT;
          this_expr->ret_type_ = RetType::FLOAT;
        } else {
          this_expr->ch0_ =
              std::make_unique<CastExpr>(std::move(this_expr->ch0_));
          this_expr->ch0_->ret_type_ = RetType::FLOAT;
          this_expr->ret_type_ = RetType::FLOAT;
        }
      } else {
        if (ltype == RetType::STRING) {
          throw PlannerException(
              "Arithmetic operators between STRINGs are invalid.");
        } else {
          this_expr->ret_type_ = ltype;
        }
      }
      if (this_expr->ret_type_ == RetType::FLOAT) {
        if (this_expr->op_ == OpType::BITAND ||
            this_expr->op_ == OpType::BITOR ||
            this_expr->op_ == OpType::BITRSH ||
            this_expr->op_ == OpType::BITLSH ||
            this_expr->op_ == OpType::BITXOR || this_expr->op_ == OpType::MOD) {
          throw PlannerException("Invalid operator between FLOATs.");
        }
      }
    } else if (expr->type_ == ExprType::BINCONDOP) {
      auto this_expr = static_cast<BinaryExpr*>(expr);
      auto retl = analysis_expr(this_expr->ch0_.get(), column_name_table);
      auto retr = analysis_expr(this_expr->ch1_.get(), column_name_table);
      result.is_constant_ = retl.is_constant_ && retr.is_constant_;
      result.aggregate_expr_num =
          retl.aggregate_expr_num + retr.aggregate_expr_num;
      auto ltype = this_expr->ch0_->ret_type_;
      auto rtype = this_expr->ch1_->ret_type_;
      this_expr->ret_type_ = RetType::INT;
      if (ltype != rtype) {
        if (ltype == RetType::STRING || rtype == RetType::STRING)
          throw PlannerException(
              "Relational operator between STRING and other type is invalid.");
        if (ltype == RetType::FLOAT) {
          this_expr->ch1_ =
              std::make_unique<CastExpr>(std::move(this_expr->ch1_));
          this_expr->ch1_->ret_type_ = RetType::FLOAT;
        } else {
          this_expr->ch0_ =
              std::make_unique<CastExpr>(std::move(this_expr->ch0_));
          this_expr->ch0_->ret_type_ = RetType::FLOAT;
        }
      } else {
        if (ltype == RetType::STRING) {
          if (this_expr->op_ != OpType::LT && this_expr->op_ != OpType::GT &&
              this_expr->op_ != OpType::LEQ && this_expr->op_ != OpType::GEQ &&
              this_expr->op_ != OpType::EQ && this_expr->op_ != OpType::NEQ) {
            throw PlannerException(
                "Relational operator between STRINGs is invalid.");
          }
        }
      }
    } else if (expr->type_ == ExprType::UNARYCONDOP) {
      auto this_expr = static_cast<UnaryConditionExpr*>(expr);
      auto ret = analysis_expr(this_expr->ch0_.get(), column_name_table);
      result = ret;
      auto ltype = this_expr->ch0_->ret_type_;
      if (ltype == RetType::STRING)
        throw PlannerException(
            "Unary conditional operator on STRING is invalid.");
      expr->ret_type_ = ltype;
    } else if (expr->type_ == ExprType::UNARYOP) {
      auto this_expr = static_cast<UnaryExpr*>(expr);
      auto ret = analysis_expr(this_expr->ch0_.get(), column_name_table);
      result = ret;
      auto ltype = this_expr->ch0_->ret_type_;
      if (ltype == RetType::STRING)
        throw PlannerException(
            "Unary arithmetic operator on STRING is invalid.");
      expr->ret_type_ = ltype;
    } else if (expr->type_ == ExprType::COLUMN) {
      auto this_expr = static_cast<ColumnExpr*>(expr);
      auto id = get_value_id(
          this_expr->table_name_, this_expr->column_name_, column_name_table);
      auto type = column_name_table[id].type_;
      this_expr->id_in_column_name_table_ = column_name_table[id].id_;
      // Get table_id
      this_expr->id_table_in_planner_ =
          table_id_table_[this_expr->id_in_column_name_table_];
      if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
        expr->ret_type_ = RetType::STRING;
      } else if (type == FieldType::INT32 || type == FieldType::INT64) {
        expr->ret_type_ = RetType::INT;
      } else {
        expr->ret_type_ = RetType::FLOAT;
      }
      result.is_constant_ = false;
      result.aggregate_expr_num = 0;
    } else if (expr->type_ == ExprType::AGGR) {
      auto this_expr = static_cast<AggregateFunctionExpr*>(expr);
      auto ret = analysis_expr(this_expr->ch0_.get(), column_name_table);
      if (ret.aggregate_expr_num > 0) {
        throw PlannerException("Aggregate functions cannot be nested");
      }
      if (this_expr->func_name_ == "avg") {
        this_expr->ret_type_ = RetType::FLOAT;
      } else if (this_expr->func_name_ == "count") {
        this_expr->ret_type_ = RetType::INT;
      } else {
        this_expr->ret_type_ = this_expr->ch0_->ret_type_;
      }
      result.is_constant_ = false;
      result.aggregate_expr_num = 1;
    } else {
      DB_ERR("Internal Error: Unknown Expression type.");
    }
    return result;
  }

  FieldType trans_to_field_type(RetType type) {
    if (type == RetType::FLOAT) {
      return FieldType::FLOAT64;
    } else if (type == RetType::INT) {
      return FieldType::INT64;
    } else if (type == RetType::STRING) {
      return FieldType::CHAR;
    } else {
      DB_ERR("Internal Error: Unrecognized RetType.");
    }
  }

  RetType trans_to_ret_type(FieldType type) {
    if (type == FieldType::INT32 || type == FieldType::INT64) {
      return RetType::INT;
    } else if (type == FieldType::FLOAT64) {
      return RetType::FLOAT;
    } else if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
      return RetType::STRING;
    } else {
      DB_ERR("Internal Error: Unrecognized FieldType.");
    }
  }

  // Check whether type y can be converted into type x without loss.
  // Because we use int64 to store int32 information in execution.
  bool IsTypeEqual(FieldType x, FieldType y) {
    if (x == FieldType::INT64) {
      return y == FieldType::INT32 || y == FieldType::INT64;
    } else if (x == FieldType::CHAR) {
      return y == FieldType::CHAR || y == FieldType::VARCHAR;
    } else {
      // FLOAT64
      return x == y;
    }
  }

  // Plan insert statement.
  // Insert statements have two types.
  // (1) insert into A values ... This inserts tuples containing constant
  // fields. (2) insert into A select ... This inserts the result of the
  // subquery, we use plan_select to plan the subquery. and store it as the
  // child plan node.
  std::unique_ptr<PlanNode> plan_insert(InsertStatement* statement) {
    auto ret = std::make_unique<InsertPlanNode>();
    auto table_id = schema_.Find(statement->table_name_);
    if (!table_id.has_value()) {
      throw PlannerException(
          fmt::format("Table \'{}\' does not exist.", statement->table_name_));
    }
    auto table_schema = schema_[table_id.value()];
    auto table_columns = table_schema.GetColumns();
    auto insert_data = plan_table(statement->insert_data_.get());
    if (insert_data->output_schema_.Size() != (table_schema.GetHidePKFlag()
                                                      ? table_columns.size() - 1
                                                      : table_columns.size())) {
      throw PlannerException("The number of fields in tuples is not correct.");
    }
    for (uint32_t index = 0; auto& a : insert_data->output_schema_.GetCols()) {
      if (!IsTypeEqual(a.type_, table_columns[index].type_)) {
        throw PlannerException(fmt::format(
            "The type of the {}-th field in insert value is not correct.",
            index + 1));
      }
      index += 1;
    }

    ret->ch_ = std::move(insert_data);
    ret->table_bitset_ = ret->ch_->table_bitset_;
    ret->table_name_ = statement->table_name_;
    table_id_table_.push_back(total_table_num_++);
    ret->output_schema_.Append(OutputColumnData{
        column_id_++, "", "inserted rows", FieldType::INT64, 0});

    return ret;
  }

  // Plan update statement.
  std::unique_ptr<PlanNode> plan_update(UpdateStatement* statement) {
    auto ret = std::make_unique<UpdatePlanNode>();
    NormalTableRef table;
    table.table_name_ = statement->table_name_;
    std::unique_ptr<PlanNode> read_from_table = plan_seq_scan(&table);
    auto table_schema = read_from_table->output_schema_;

    for (auto& a : statement->other_tables_) {
      read_from_table =
          join_two_plan(std::move(read_from_table), plan_table(a.get()));
    }

    for (auto& a : statement->updates_) {
      auto expr_result = analysis_expr(
          a->update_value_.get(), read_from_table->output_schema_);
      if (expr_result.aggregate_expr_num > 0) {
        throw PlannerException(
            "We do not support aggregate functions in update statements.");
      }
      ret->updates_.emplace_back(
          get_value_id(a->table_name_, a->column_name_, table_schema),
          a->update_value_->clone());
    }

    if (statement->predicate_) {
      ret->ch_ =
          add_filter(std::move(read_from_table), statement->predicate_.get());
    } else {
      ret->ch_ = std::move(read_from_table);
    }

    ret->table_bitset_ = ret->ch_->table_bitset_;
    ret->table_name_ = statement->table_name_;
    table_id_table_.push_back(total_table_num_++);
    ret->output_schema_.Append(OutputColumnData{
        column_id_++, "", "updated rows", FieldType::INT64, 0});

    return ret;
  }

  // Plan delete statement.
  // We first read all tuples from the table and determine the obselete tuples.
  // And delete them after reads.
  std::unique_ptr<PlanNode> plan_delete(DeleteStatement* statement) {
    auto ret = std::make_unique<DeletePlanNode>();
    NormalTableRef table;
    table.table_name_ = statement->table_name_;
    auto seq_scan_plan = plan_seq_scan(&table);

    if (statement->predicate_) {
      ret->ch_ =
          add_filter(std::move(seq_scan_plan), statement->predicate_.get());
    } else {
      ret->ch_ = std::move(seq_scan_plan);
    }
    ret->table_name_ = statement->table_name_;
    ret->table_bitset_ = ret->ch_->table_bitset_;
    table_id_table_.push_back(total_table_num_++);
    ret->output_schema_.Append(OutputColumnData{
        column_id_++, "", "deleted rows", FieldType::INT64, 0});
    return ret;
  }

  std::unique_ptr<PlanNode> plan_select(SelectStatement* statement) {
    std::unique_ptr<PlanNode> ret = nullptr;

    // The plan node that returns columns from referred tables.
    std::unique_ptr<PlanNode> read_from_tables = nullptr;

    // Generate plans for tables and generate a naive join plan over them (i.e.
    // connect them one by one)

    for (auto& a : statement->tables_) {
      auto plan = plan_table(a.get());
      if (!read_from_tables)
        read_from_tables = std::move(plan);
      else
        read_from_tables =
            join_two_plan(std::move(read_from_tables), std::move(plan));
    }

    // The output schema of read_from_tables are the concats of
    // table_schema.GetStorageColumn() But if we use * in result columns, we
    // want the concats of table_schema.GetColumn() This is the concats of
    // table_schema.GetStorageColumn().
    OutputSchema concat_get_column;
    if (read_from_tables) {
      concat_get_column.GetCols().reserve(
          read_from_tables->output_schema_.Size());
      get_table_schema_concat(read_from_tables.get(), concat_get_column);
    }

    // For every result column, if it is * (type_ == ResultColumnType::ALL),
    // then we enumerate all the columns of tables and add ColumnExpr to
    // output_exprs_ Otherwise, its type is ResultColumnType::EXPR We first call
    // analysis_expr to analysis the return type and variables. Then add it to
    // column_name_table_ using column name _#1, _#2...

    std::vector<std::unique_ptr<Expr>> output_exprs;
    OutputSchema output_schema;

    // The result of ProjectPlanNode is std::vector<StaticFieldRef>
    // It is not raw tuple data.
    output_schema.SetRaw(false);

    // Check whether it has aggregate functions (then we should use
    // AggregatePlanNode)
    bool aggregate_flag = false;

    bool has_non_trivial_output = false;
    auto current_table_num = total_table_num_;

    // Do not check whether the column names are the same. Unless they are used
    // in expressions. For example, we allow:
    //      select * from A, A;
    //      select * from (select 1 as b) as c, (select 2 as b) as c;
    // We do not allow:
    //      select a + b from (select 1 as b) as c, (select 1 as b) as c,
    //      (select 2 as a) as c;
    // (here b is ambiguous.)
    for (auto& a : statement->result_column_) {
      if (a->type_ == ResultColumnType::ALL) {
        if (a->as_ != "")
          DB_ERR("Internal Error: \'*\' cannot have alias.");
        if (!read_from_tables)
          throw PlannerException("Variables exist but no input tables.");
        output_schema.Append(concat_get_column);
        for (auto& output_column : concat_get_column.GetCols()) {
          auto expr = std::make_unique<ColumnExpr>(
              output_column.table_name_, output_column.column_name_);
          expr->id_in_column_name_table_ = output_column.id_;
          expr->id_table_in_planner_ = table_id_table_[output_column.id_];
          expr->ret_type_ = trans_to_ret_type(output_column.type_);
          output_exprs.push_back(std::move(expr));
        }
      } else if (a->type_ == ResultColumnType::EXPR) {
        auto expr = static_cast<ExprResultColumn*>(a.get());
        // Use column names from referred tables and do not use aliases from
        // previous result column. An example:
        //      select 1 as a, a + 1;
        // This is wrong.
        //      select 1 as a, a + 1 from A;
        // This is correct. (There exists a column named 'a' in table A.)
        if (expr->expr_->type_ == ExprType::COLUMN) {
          auto this_expr = static_cast<ColumnExpr*>(expr->expr_.get());
          auto id =
              get_value_id(this_expr->table_name_, this_expr->column_name_,
                  read_from_tables ? read_from_tables->output_schema_
                                   : OutputSchema());
          if (a->as_ == "") {
            // read_from_tables must be not nullptr. Otherwise it must throw an
            // exception before.
            output_schema.Append(read_from_tables->output_schema_[id]);
          } else {
            auto data = read_from_tables->output_schema_[id];
            data.column_name_ = a->as_;
            output_schema.Append(data);
          }
          this_expr->ret_type_ =
              trans_to_ret_type(read_from_tables->output_schema_[id].type_);
          this_expr->id_in_column_name_table_ =
              read_from_tables->output_schema_[id].id_;
          this_expr->id_table_in_planner_ =
              table_id_table_[read_from_tables->output_schema_[id].id_];
        } else {
          auto expr_result = analysis_expr(expr->expr_.get(),
              read_from_tables ? read_from_tables->output_schema_
                               : OutputSchema());
          aggregate_flag |= expr_result.aggregate_expr_num > 0;
          auto column_name =
              a->as_ == "" ? fmt::format("_#{}", ++unname_col_) : a->as_;
          // size_ = 0 means that the field is a StaticFieldRef.
          // It is calculated and stored in an allocated memory.
          // size_ > 0 means that the field is in a tuple which is stored in the
          // raw page and has not been deserialized.
          if (!has_non_trivial_output) {
            has_non_trivial_output = true;
            total_table_num_++;
          }
          table_id_table_.push_back(current_table_num);
          output_schema.Append(OutputColumnData{column_id_++, "", column_name,
              trans_to_field_type(expr->expr_->ret_type_), 0});
        }
        output_exprs.push_back(expr->expr_->clone());
      }
    }

    if (read_from_tables == nullptr) {
      // Ensure that ProjectPlanNode don't read the table
      auto table = std::make_unique<PrintPlanNode>();
      table->num_fields_per_tuple_ = 1;
      table->values_ = std::make_shared<StaticFieldVector>(
          std::vector<Field>{Field::CreateInt(FieldType::INT64, 8, 0)});

      table_id_table_.push_back(current_table_num);
      table->output_schema_.Append(
          OutputColumnData{column_id_++, "", "unused", FieldType::INT64, 0});
      read_from_tables = std::move(table);
    }

    // The schema containing result columns of referred tables.
    auto input_schema = read_from_tables->output_schema_;

    // If there is a predicate (where clause), create a filter on
    // read_from_tables.
    if (statement->predicate_) {
      read_from_tables =
          add_filter(std::move(read_from_tables), statement->predicate_.get());
    }

    // Allow aggregate although there is no aggregate functions.
    // For example,
    //    select name from users group by country;
    aggregate_flag |= statement->group_by_.size() > 0;

    // Group by clause can be used even when there is no aggregate functions in
    // result columns. p.s. In MySQL grammar, group by 1 means group by the
    // first result column, But here it means the integer 1.
    if (aggregate_flag) {
      auto aggr_plan = std::make_unique<AggregatePlanNode>();
      for (auto& a : statement->group_by_) {
        auto expr_result = analysis_expr(a.get(), input_schema);
        if (expr_result.aggregate_expr_num > 0) {
          throw PlannerException(
              "Aggregate functions cannot be in group by clause.");
        }
        aggr_plan->group_by_exprs_.push_back(a->clone());
      }
      aggr_plan->table_bitset_ = read_from_tables->table_bitset_;
      if (statement->having_) {
        analysis_expr(statement->having_.get(), input_schema);
        aggr_plan->group_predicate_ =
            PredicateVec::Create(statement->having_.get());
      }
      aggr_plan->ch_ = std::move(read_from_tables);
      aggr_plan->output_schema_ = std::move(output_schema);
      aggr_plan->output_exprs_ = std::move(output_exprs);
      ret = std::move(aggr_plan);
    } else {
      // Ordinary select clause
      auto project_plan = std::make_unique<ProjectPlanNode>();
      project_plan->table_bitset_ = read_from_tables->table_bitset_;
      project_plan->ch_ = std::move(read_from_tables);
      project_plan->output_schema_ = std::move(output_schema);
      project_plan->output_exprs_ = std::move(output_exprs);
      ret = std::move(project_plan);
    }

    // We evaluate the order by expression in ProjectPlanNode or
    // AggregatePlanNode.
    if (statement->order_by_.size()) {
      auto order_plan = std::make_unique<OrderByPlanNode>();
      std::vector<std::unique_ptr<Expr>> order_by_value_exprs;
      OutputSchema new_output_schema;
      std::vector<std::pair<RetType, bool>> order_by_pairs;
      for (auto& a : statement->order_by_) {
        auto expr_result = analysis_expr(a->expr_.get(), input_schema);
        if (expr_result.aggregate_expr_num > 0 && !aggregate_flag) {
          throw PlannerException(
              "Aggregate functions cannot be in order by clause unless there "
              "exists group by clause.");
        }
        // Create new expressions for OrderBy.
        order_by_value_exprs.push_back(a->expr_->clone());
        auto column_name = fmt::format("_#{}", ++unname_col_);
        table_id_table_.push_back(current_table_num);
        new_output_schema.Append(OutputColumnData{column_id_++, "", column_name,
            trans_to_field_type(a->expr_->ret_type_), 0});

        order_by_pairs.push_back({a->expr_->ret_type_, a->is_asc_});
      }
      order_plan->output_schema_ = ret->output_schema_;
      order_plan->order_by_offset_ = new_output_schema.Size();
      // Add order by value columns in front of other columns.
      ret->output_schema_.GetCols().insert(
          ret->output_schema_.GetCols().begin(),
          std::make_move_iterator(new_output_schema.GetCols().begin()),
          std::make_move_iterator(new_output_schema.GetCols().end()));
      if (aggregate_flag) {
        static_cast<AggregatePlanNode*>(ret.get())->output_exprs_.insert(
            static_cast<AggregatePlanNode*>(ret.get())->output_exprs_.begin(),
            std::make_move_iterator(order_by_value_exprs.begin()),
            std::make_move_iterator(order_by_value_exprs.end()));

      } else {
        static_cast<ProjectPlanNode*>(ret.get())->output_exprs_.insert(
            static_cast<ProjectPlanNode*>(ret.get())->output_exprs_.begin(),
            std::make_move_iterator(order_by_value_exprs.begin()),
            std::make_move_iterator(order_by_value_exprs.end()));
      }
      order_plan->table_bitset_ = ret->table_bitset_;
      order_plan->ch_ = std::move(ret);
      order_plan->order_by_exprs_ = std::move(order_by_pairs);
      ret = std::move(order_plan);
    }

    if (statement->is_distinct_) {
      auto d_plan = std::make_unique<DistinctPlanNode>();
      d_plan->output_schema_ = ret->output_schema_;
      d_plan->table_bitset_ = ret->table_bitset_;
      d_plan->ch_ = std::move(ret);
      ret = std::move(d_plan);
    }

    // We only support constant limit count and limit offset.
    if (statement->limit_count_) {
      auto limit_count_result =
          analysis_expr(statement->limit_count_.get(), input_schema);
      if (!limit_count_result.is_constant_) {
        throw PlannerException("We only support constant limit count.");
      }
      // Has been constant folded.
      if (statement->limit_count_->ret_type_ != RetType::INT ||
          statement->limit_count_->type_ != ExprType::LITERAL_INTEGER) {
        throw PlannerException("Limit count must be integer.");
      }
      auto limit_plan = std::make_unique<LimitPlanNode>();
      limit_plan->limit_size_ =
          static_cast<const LiteralIntegerExpr*>(statement->limit_count_.get())
              ->literal_value_;
      if (statement->limit_offset_) {
        auto limit_offset_result =
            analysis_expr(statement->limit_offset_.get(), input_schema);
        if (!limit_offset_result.is_constant_) {
          throw PlannerException("We only support constant limit offset.");
        }
        // Has been constant folded.
        if (statement->limit_offset_->ret_type_ != RetType::INT ||
            statement->limit_count_->type_ != ExprType::LITERAL_INTEGER) {
          throw PlannerException("Limit offset must be integer.");
        }
        limit_plan->offset_ = static_cast<const LiteralIntegerExpr*>(
            statement->limit_offset_.get())
                                  ->literal_value_;
      }
      limit_plan->ch_ = std::move(ret);
      limit_plan->table_bitset_ = limit_plan->ch_->table_bitset_;
      limit_plan->output_schema_ = limit_plan->ch_->output_schema_;
      ret = std::move(limit_plan);
    }
    return ret;
  }
  // Plan seq scan of a table. It records the table name. The table name will be
  // used to get corresponding iterators from DB. It adds all the columns to the
  // column name table.
  std::unique_ptr<SeqScanPlanNode> plan_seq_scan(NormalTableRef* table) {
    auto ret = std::make_unique<SeqScanPlanNode>();
    ret->table_name_ = table->table_name_;
    auto table_id = schema_.Find(table->table_name_);
    if (!table_id.has_value())
      throw PlannerException(
          fmt::format("Table \'{}\' does not exist.", table->table_name_));
    auto& table_data = schema_[table_id.value()];
    // Create a bit for this table
    total_table_num_++;
    ret->table_bitset_ = BitVector(total_table_num_);
    ret->table_bitset_[total_table_num_ - 1] = 1;
    // Maybe we can do this more gracefully...
    ret->output_schema_.GetCols().resize(table_data.GetStorageColumns().size());
    for (uint32_t index = 0; const auto& a : table_data.GetStorageColumns()) {
      table_id_table_.push_back(total_table_num_ - 1);
      ret->output_schema_[index] = OutputColumnData{
          column_id_++, ret->table_name_, a.name_, a.type_, a.size_};
      index += 1;
    }
    ret->output_schema_.SetRaw(true);
    return ret;
  }
  // Add a filter on a plan node.
  std::unique_ptr<PlanNode> add_filter(
      std::unique_ptr<PlanNode> child, Expr* expr) {
    auto ret = std::make_unique<FilterPlanNode>();
    ret->output_schema_ = child->output_schema_;
    analysis_expr(expr, child->output_schema_);
    // The return value of expr can only be RetType::INT
    if (expr->ret_type_ != RetType::INT) {
      throw PlannerException("Return value of predicate can only be integer.");
    }
    ret->ch_ = std::move(child);
    ret->table_bitset_ = ret->ch_->table_bitset_;
    ret->predicate_ = PredicateVec::Create(expr);
    return ret;
  }
  // Plan join of two tables. The output schema is the union of the schemas of
  // two tables. If the on clause is not specified, the predicate is set to be
  // nullptr.
  std::unique_ptr<PlanNode> plan_join(JoinTableRef* table) {
    auto ret = std::make_unique<JoinPlanNode>();
    ret->ch_ = plan_table(table->ch_[0].get());
    ret->ch2_ = plan_table(table->ch_[1].get());
    ret->output_schema_ = OutputSchema::Concat(
        ret->ch_->output_schema_, ret->ch2_->output_schema_);
    // The output of join is std::vector<StaticFieldRef>.
    ret->output_schema_.SetRaw(false);
    ret->table_bitset_ = ret->ch_->table_bitset_ | ret->ch2_->table_bitset_;
    if (table->predicate_) {
      return add_filter(std::move(ret), table->predicate_.get());
    }
    return ret;
  }
  // Plan values table. Check if field types of all the tuples are the same.
  std::unique_ptr<PlanNode> plan_values_table(ValuesTableRef* table) {
    auto ret = std::make_unique<PrintPlanNode>();
    ret->values_ = std::make_shared<StaticFieldVector>(table->values_);
    ret->num_fields_per_tuple_ = table->num_fields_per_tuple_;
    // Create a bit for this table. Although it is not physical table...
    auto current_table_num = total_table_num_++;
    ret->table_bitset_ = BitVector(current_table_num + 1);
    ret->table_bitset_[current_table_num] = 1;
    if (table->num_fields_per_tuple_ == 0) {
      DB_ERR("Internal Error: The number of fields per tuple cannot be 0.");
    }
    for (uint32_t index = 0; auto& a : table->values_) {
      if (index < table->num_fields_per_tuple_) {
        table_id_table_.push_back(current_table_num);
        ret->output_schema_.Append(OutputColumnData{
            column_id_++, "", fmt::format("_#{}", ++unname_col_), a.type_, 0});
      } else {
        if (!IsTypeEqual(a.type_,
                ret->output_schema_[index % table->num_fields_per_tuple_]
                    .type_)) {
          throw PlannerException(
              fmt::format("The type of the {}-th field in the {}-th tuple in "
                          "values clause is not correct.",
                  index % table->num_fields_per_tuple_ + 1,
                  index / table->num_fields_per_tuple_ + 1));
        }
      }
      index += 1;
    }
    return ret;
  }
  // Plan the 4 types of tables.
  std::unique_ptr<PlanNode> plan_table(TableRef* table) {
    std::unique_ptr<PlanNode> ret;
    if (table->table_type_ == TableRefType::TABLE) {
      ret = plan_seq_scan(static_cast<NormalTableRef*>(table));
    } else if (table->table_type_ == TableRefType::JOIN) {
      ret = plan_join(static_cast<JoinTableRef*>(table));
    } else if (table->table_type_ == TableRefType::SUBQUERY) {
      // Subqueries can have no alias.
      ret = plan_select(static_cast<SubqueryTableRef*>(table)->ch_.get());
    } else if (table->table_type_ == TableRefType::VALUES) {
      ret = plan_values_table(static_cast<ValuesTableRef*>(table));
    } else {
      DB_ERR("Internal Error: Unrecognized table type.");
      return nullptr;
    }
    if (table->as_ != nullptr) {
      if (table->as_->column_names_.size() == 0) {
        for (auto& col : ret->output_schema_.GetCols())
          col.table_name_ = table->as_->table_name_;
      } else {
        if (ret->output_schema_.Size() != table->as_->column_names_.size()) {
          throw PlannerException(
              "Number of columns in the AS clause is not correct.");
        }
        for (auto index = 0; auto& col : ret->output_schema_.GetCols()) {
          col.table_name_ = table->as_->table_name_;
          col.column_name_ = table->as_->column_names_[index];
          index += 1;
        }
      }
    }
    return ret;
  }

  // Produce the schema of '*'.
  void get_table_schema_concat(PlanNode* table, OutputSchema& vec) {
    if (table->type_ == PlanType::SeqScan) {
      auto this_table = static_cast<SeqScanPlanNode*>(table);
      auto table_id = schema_.Find(this_table->table_name_);
      auto& table_data = schema_[table_id.value()];
      OutputSchema this_vec;
      // If the primary key needs to be hide.
      if (table_data.GetHidePKFlag()) {
        this_vec.GetCols().reserve(this_table->output_schema_.Size() - 1);
        for (uint32_t i = 0; i < this_table->output_schema_.Size(); i++)
          if (i != table_data.GetPrimaryKeyIndex()) {
            this_vec.GetCols().push_back(
                this_table
                    ->output_schema_[table_data.GetShuffleToStorage()[i]]);
          }
      } else {
        this_vec.GetCols().resize(this_table->output_schema_.Size());
        for (uint32_t i = 0; i < this_table->output_schema_.Size(); i++) {
          this_vec[i] =
              this_table->output_schema_[table_data.GetShuffleToStorage()[i]];
        }
      }

      vec.Append(std::move(this_vec));
    } else if (table->type_ == PlanType::Join) {
      auto this_table = static_cast<JoinPlanNode*>(table);
      get_table_schema_concat(this_table->ch_.get(), vec);
      get_table_schema_concat(this_table->ch2_.get(), vec);
    } else if (table->type_ == PlanType::Project ||
               table->type_ == PlanType::Aggregate ||
               table->type_ == PlanType::Print ||
               table->type_ == PlanType::Order) {
      vec.Append(table->output_schema_);
    } else if (table->type_ == PlanType::Filter) {
      return get_table_schema_concat(
          static_cast<FilterPlanNode*>(table)->ch_.get(), vec);
    } else if (table->type_ == PlanType::Limit) {
      return get_table_schema_concat(
          static_cast<LimitPlanNode*>(table)->ch_.get(), vec);
    } else if (table->type_ == PlanType::Distinct) {
      return get_table_schema_concat(
          static_cast<DistinctPlanNode*>(table)->ch_.get(), vec);
    } else {
      DB_ERR("Internal Error: Unrecognized plan type.");
    }
  }

  /** In from clause, we join the listed tables one by one.
   * For example, in the following statement
   *    select * from A, B, C;
   * We generate a join plan like:
   *        Join
   *        /  \
   *      Join  C
   *      /   \
   *    A     B
   */
  std::unique_ptr<PlanNode> join_two_plan(
      std::unique_ptr<PlanNode> ch0, std::unique_ptr<PlanNode> ch1) {
    auto ret = std::make_unique<JoinPlanNode>();
    ret->ch_ = std::move(ch0);
    ret->ch2_ = std::move(ch1);
    ret->output_schema_ = OutputSchema::Concat(
        ret->ch_->output_schema_, ret->ch2_->output_schema_);
    ret->output_schema_.SetRaw(false);
    ret->table_bitset_ = ret->ch_->table_bitset_ | ret->ch2_->table_bitset_;
    return ret;
  }

 private:
  const DBSchema& schema_;
  uint32_t unname_col_{0};
  uint32_t column_id_{0};
  uint32_t total_table_num_{0};
  // Get table_id using id_in_column_table_
  std::vector<uint32_t> table_id_table_;
};

BasicPlanGenerator::BasicPlanGenerator(const DBSchema& schema) {
  ptr_ = std::make_unique<Impl>(schema);
}

BasicPlanGenerator::~BasicPlanGenerator() {}

std::pair<std::unique_ptr<PlanNode>, std::string> BasicPlanGenerator::Plan(
    Statement* statement) {
  return ptr_->Plan(statement);
}

// Print

static const char* op_str[] = {"+", "-", "*", "/", "%", "&", "^", "|", "<<",
    ">>", "<", ">", "<=", ">=", "=", "<>", "and", "or", "not", "-"};

static const char* return_type_str[] = {"int", "float", "string", "null"};

static const char* field_type_str[] = {
    "int32", "int64", "float64", "char", "varchar", "empty"};

std::string AddSpacesAfterNewLine(std::string_view s, size_t num) {
  std::string ret;
  for (auto a : s) {
    ret += a;
    if (a == '\n') {
      for (size_t i = 0; i < num; i++)
        ret += ' ';
    }
  }
  return ret;
}

template <typename T, typename F>
std::string VecToString(const std::vector<T>& v, F&& func) {
  std::string ret;
  for (size_t i = 0; i < v.size(); i++) {
    ret += func(v[i]);
    if (i != v.size() - 1)
      ret += ", ";
  }
  return ret;
}

std::string ProjectPlanNode::ToString() const {
  int i = 0;
  return fmt::format("Project [Output: {}] \n  -> {}",
      VecToString(output_exprs_,
          [&](const std::unique_ptr<Expr>& x) {
            i++;
            return fmt::format("{}%{}={}", output_schema_[i - 1].column_name_,
                output_schema_[i - 1].id_, x->ToString());
          }),
      AddSpacesAfterNewLine(ch_->ToString(), 4));
}

std::string FilterPlanNode::ToString() const {
  return fmt::format("Filter [Predicate: {}] \n  -> {}", predicate_.ToString(),
      AddSpacesAfterNewLine(ch_->ToString(), 4));
}

std::string SeqScanPlanNode::ToString() const {
  return fmt::format("Seq Scan [Table: {}] [Predicate: {}]", table_name_,
      predicate_.ToString());
}

std::string JoinPlanNode::ToString() const {
  return fmt::format("Join [Predicate: {}] \n  -> {}\n  -> {}",
      predicate_.ToString(), AddSpacesAfterNewLine(ch_->ToString(), 4),
      AddSpacesAfterNewLine(ch2_->ToString(), 4));
}

std::string HashJoinPlanNode::ToString() const {
  return fmt::format(
      "Join [Predicate: {}] \n  [Hash Keys: {}]\n  -> {}\n  [Hash Keys: {}]\n  "
      "-> {}",
      predicate_.ToString(),
      VecToString(left_hash_exprs_,
          [&](const std::unique_ptr<Expr>& x) { return x->ToString(); }),
      AddSpacesAfterNewLine(ch_->ToString(), 4),
      VecToString(right_hash_exprs_,
          [&](const std::unique_ptr<Expr>& x) { return x->ToString(); }),
      AddSpacesAfterNewLine(ch2_->ToString(), 4));
}

std::string AggregatePlanNode::ToString() const {
  int i = 0;
  return fmt::format(
      "Aggregate [Group by: {}] [Group predicate: {}] [Output: {}] \n  -> {}",
      VecToString(group_by_exprs_,
          [](const std::unique_ptr<Expr>& x) { return x->ToString(); }),
      group_predicate_.ToString(),
      VecToString(output_exprs_,
          [&](const std::unique_ptr<Expr>& x) {
            i++;
            return fmt::format("{}%{}={}", output_schema_[i - 1].column_name_,
                output_schema_[i - 1].id_, x->ToString());
          }),
      AddSpacesAfterNewLine(ch_->ToString(), 4));
}

std::string OrderByPlanNode::ToString() const {
  int i = 0;
  return fmt::format("Sort [On: {}] \n  -> {}",
      VecToString(order_by_exprs_,
          [&](const std::pair<RetType, bool>& x) {
            auto expr =
                std::make_unique<ColumnExpr>(ch_->output_schema_[i].table_name_,
                    ch_->output_schema_[i].column_name_);
            expr->id_in_column_name_table_ = ch_->output_schema_[i].id_;
            expr->ret_type_ = x.first;
            i++;
            return fmt::format(
                "{} {}", expr->ToString(), x.second ? "asc" : "desc");
          }),
      AddSpacesAfterNewLine(ch_->ToString(), 4));
}

std::string LimitPlanNode::ToString() const {
  return fmt::format("Limit [Limit {}, Offset {}] \n  -> {}", limit_size_,
      offset_, AddSpacesAfterNewLine(ch_->ToString(), 4));
}

std::string InsertPlanNode::ToString() const {
  return fmt::format(
      "Insert \n  -> {}", AddSpacesAfterNewLine(ch_->ToString(), 4));
}

std::string UpdatePlanNode::ToString() const {
  return fmt::format("Update [{}] \n  -> {}",
      VecToString(updates_,
          [](const std::pair<uint32_t, std::unique_ptr<Expr>>& x) {
            return fmt::format("Col%{}={}", x.first, x.second->ToString());
          }),
      AddSpacesAfterNewLine(ch_->ToString(), 4));
}

std::string DeletePlanNode::ToString() const {
  return fmt::format(
      "Delete \n  -> {}", AddSpacesAfterNewLine(ch_->ToString(), 4));
}

std::string PrintPlanNode::ToString() const {
  return fmt::format("Print Values [{} tuples, {} fields per tuple]",
      values_->GetFieldVector().size() / num_fields_per_tuple_,
      num_fields_per_tuple_);
}

std::string DistinctPlanNode::ToString() const {
  return fmt::format(
      "Distinct \n  -> {}", AddSpacesAfterNewLine(ch_->ToString(), 4));
}

std::string RangeScanPlanNode::ToString() const {
  return fmt::format(
      "Range Scan [Table: {}] [Range: {}{}, {}{} ] [Predicate: {}]",
      table_name_, range_l_.second ? "[" : "(", range_l_.first.ToString(),
      range_r_.first.ToString(), range_r_.second ? "]" : ")",
      predicate_.ToString());
}

std::unique_ptr<PlanNode> ProjectPlanNode::clone() const {
  auto ret = std::make_unique<ProjectPlanNode>();
  ret->output_schema_ = output_schema_;
  ret->ch2_ = ch2_ ? ch2_->clone() : nullptr;
  ret->ch_ = ch_ ? ch_->clone() : nullptr;
  for (auto& a : output_exprs_)
    ret->output_exprs_.push_back(a->clone());
  ret->table_bitset_ = table_bitset_;
  return ret;
}

std::unique_ptr<PlanNode> FilterPlanNode::clone() const {
  auto ret = std::make_unique<FilterPlanNode>();
  ret->output_schema_ = output_schema_;
  ret->ch2_ = ch2_ ? ch2_->clone() : nullptr;
  ret->ch_ = ch_ ? ch_->clone() : nullptr;
  ret->predicate_ = predicate_.clone();
  ret->table_bitset_ = table_bitset_;
  return ret;
}

std::unique_ptr<PlanNode> SeqScanPlanNode::clone() const {
  auto ret = std::make_unique<SeqScanPlanNode>();
  ret->output_schema_ = output_schema_;
  ret->table_name_ = table_name_;
  ret->table_bitset_ = table_bitset_;
  ret->predicate_ = predicate_.clone();
  return ret;
}

std::unique_ptr<PlanNode> JoinPlanNode::clone() const {
  auto ret = std::make_unique<JoinPlanNode>();
  ret->output_schema_ = output_schema_;
  ret->predicate_ = predicate_.clone();
  ret->ch2_ = ch2_ ? ch2_->clone() : nullptr;
  ret->ch_ = ch_ ? ch_->clone() : nullptr;
  ret->table_bitset_ = table_bitset_;
  return ret;
}

std::unique_ptr<PlanNode> AggregatePlanNode::clone() const {
  auto ret = std::make_unique<AggregatePlanNode>();
  ret->output_schema_ = output_schema_;
  ret->group_predicate_ = group_predicate_.clone();
  ret->ch2_ = ch2_ ? ch2_->clone() : nullptr;
  ret->ch_ = ch_ ? ch_->clone() : nullptr;
  for (auto& a : output_exprs_)
    ret->output_exprs_.push_back(a->clone());
  for (auto& a : group_by_exprs_)
    ret->group_by_exprs_.push_back(a->clone());
  ret->table_bitset_ = table_bitset_;
  return ret;
}

std::unique_ptr<PlanNode> OrderByPlanNode::clone() const {
  auto ret = std::make_unique<OrderByPlanNode>();
  ret->output_schema_ = output_schema_;
  ret->order_by_exprs_ = order_by_exprs_;
  ret->ch2_ = ch2_ ? ch2_->clone() : nullptr;
  ret->ch_ = ch_ ? ch_->clone() : nullptr;
  ret->table_bitset_ = table_bitset_;
  ret->order_by_offset_ = order_by_offset_;
  return ret;
}

std::unique_ptr<PlanNode> LimitPlanNode::clone() const {
  auto ret = std::make_unique<LimitPlanNode>();
  ret->output_schema_ = output_schema_;
  ret->limit_size_ = limit_size_;
  ret->offset_ = offset_;
  ret->ch2_ = ch2_ ? ch2_->clone() : nullptr;
  ret->ch_ = ch_ ? ch_->clone() : nullptr;
  ret->table_bitset_ = table_bitset_;
  return ret;
}

std::unique_ptr<PlanNode> InsertPlanNode::clone() const {
  auto ret = std::make_unique<InsertPlanNode>();
  ret->output_schema_ = output_schema_;
  ret->table_name_ = table_name_;
  ret->ch2_ = ch2_ ? ch2_->clone() : nullptr;
  ret->ch_ = ch_ ? ch_->clone() : nullptr;
  ret->table_bitset_ = table_bitset_;
  return ret;
}

std::unique_ptr<PlanNode> UpdatePlanNode::clone() const {
  auto ret = std::make_unique<UpdatePlanNode>();
  ret->output_schema_ = output_schema_;
  ret->table_name_ = table_name_;
  for (auto& [col, expr] : updates_)
    ret->updates_.push_back({col, expr->clone()});
  ret->ch2_ = ch2_ ? ch2_->clone() : nullptr;
  ret->ch_ = ch_ ? ch_->clone() : nullptr;
  ret->table_bitset_ = table_bitset_;
  return ret;
}

std::unique_ptr<PlanNode> DeletePlanNode::clone() const {
  auto ret = std::make_unique<DeletePlanNode>();
  ret->output_schema_ = output_schema_;
  ret->table_name_ = table_name_;
  ret->ch2_ = ch2_ ? ch2_->clone() : nullptr;
  ret->ch_ = ch_ ? ch_->clone() : nullptr;
  ret->table_bitset_ = table_bitset_;
  return ret;
}

std::unique_ptr<PlanNode> PrintPlanNode::clone() const {
  auto ret = std::make_unique<PrintPlanNode>();
  ret->output_schema_ = output_schema_;
  ret->values_ = values_;
  ret->num_fields_per_tuple_ = num_fields_per_tuple_;
  ret->table_bitset_ = table_bitset_;
  return ret;
}

std::unique_ptr<PlanNode> DistinctPlanNode::clone() const {
  auto ret = std::make_unique<DistinctPlanNode>();
  ret->output_schema_ = output_schema_;
  ret->ch2_ = ch2_ ? ch2_->clone() : nullptr;
  ret->ch_ = ch_ ? ch_->clone() : nullptr;
  ret->table_bitset_ = table_bitset_;
  return ret;
}

std::unique_ptr<PlanNode> HashJoinPlanNode::clone() const {
  auto ret = std::make_unique<HashJoinPlanNode>();
  ret->output_schema_ = output_schema_;
  ret->predicate_ = predicate_.clone();
  ret->ch2_ = ch2_ ? ch2_->clone() : nullptr;
  ret->ch_ = ch_ ? ch_->clone() : nullptr;
  for (auto& a : left_hash_exprs_)
    ret->left_hash_exprs_.push_back(a->clone());
  for (auto& a : right_hash_exprs_)
    ret->right_hash_exprs_.push_back(a->clone());
  ret->table_bitset_ = table_bitset_;
  return ret;
}

std::unique_ptr<PlanNode> RangeScanPlanNode::clone() const {
  auto ret = std::make_unique<RangeScanPlanNode>();
  ret->output_schema_ = output_schema_;
  ret->table_name_ = table_name_;
  ret->table_bitset_ = table_bitset_;
  ret->predicate_ = predicate_.clone();
  ret->range_l_ = range_l_;
  ret->range_r_ = range_r_;
  return ret;
}

std::string ColumnSchema::ToString() const {
  return fmt::format(
      "{} {}({})", name_, field_type_str[static_cast<uint32_t>(type_)], size_);
}

std::string ForeignKeySchema::ToString() const {
  return fmt::format("{} -> {}({})", name_, table_name_, column_name_);
}

std::string TableSchema::ToString() const {
  return fmt::format("{} [{}] primary key [{}] foreign key [{}]", name_,
      VecToString(
          columns_, [&](const ColumnSchema& x) { return x.ToString(); }),
      columns_[pk_index_].name_,
      VecToString(
          fk_, [&](const ForeignKeySchema& x) { return x.ToString(); }));
}

}  // namespace wing
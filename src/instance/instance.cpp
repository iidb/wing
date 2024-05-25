#include "instance/instance.hpp"

#include <iostream>
#include <memory>

#include "catalog/db.hpp"
#include "common/cmdline.hpp"
#include "common/exception.hpp"
#include "common/logging.hpp"
#include "common/stopwatch.hpp"
#include "execution/executor.hpp"
#include "parser/parser.hpp"
#include "plan/optimizer.hpp"
#include "transaction/txn.hpp"
#include "transaction/txn_manager.hpp"
#include "type/tuple.hpp"

namespace wing {

class Instance::Impl {
 public:
  Impl(std::string_view db_file, WingOptions options)
    : options_(options), db_(db_file, options_) {}
  void ExecuteShell() {
    auto& out = std::cout;
    auto& err = std::cerr;
    SQLCmdLine cmd;
    cmd.SetCommand("exit", [](std::string_view) -> bool { return false; });
    cmd.SetCommand("quit", [](std::string_view) -> bool { return false; });
    cmd.SetCommand("explain", [&](std::string_view statement) -> bool {
      StopWatch watch;
      auto ret = parser_.Parse(statement, db_.GetDBSchema());
      err << fmt::format(
          "Parsing completed in {} seconds.\n", watch.GetTimeInSeconds());
      watch.Reset();
      if (!ret.Valid()) {
        err << ret.GetErrorMsg() << std::endl;
      } else if (ret.GetPlan() != nullptr) {
        out << ret.GetAST()->ToString() << std::endl;
        out << "=======================" << std::endl;
        auto plan = ret.GetPlan()->clone();
        plan = LogicalOptimizer::Optimize(std::move(plan), db_);
        plan = CostBasedOptimizer::Optimize(std::move(plan), db_);
        out << plan->ToString() << std::endl;
      } else {
        out << ret.GetAST()->ToString() << std::endl;
        out << "=======================" << std::endl;
        out << "(Metadata operation has no plan)" << std::endl;
      }
      return true;
    });

    // show table: show all tables.
    // show index: show all indexes. (But we don't have indexes now, haha.)
    cmd.SetCommand("show", [&](std::string_view command) -> bool {
      uint32_t c = 0;
      while (c < command.size() && isspace(command[c]))
        c++;
      if (command.substr(c, 5) == "table") {
        for (auto& tab : db_.GetDBSchema().GetTables()) {
          out << tab.ToString() << std::endl;
        }
      } else if (command.substr(c, 5) == "index") {
        out << "Not supported" << std::endl;
      }
      return true;
    });

    // analyze <table> Refresh the statistics.
    cmd.SetCommand("analyze", [&](std::string_view command) -> bool {
      uint32_t c = 0, cend = 0;
      while (c < command.size() && isspace(command[c]))
        c++;
      cend = c;
      if (command[c] == '\"') {
        c++;
        cend++;
        while (cend < command.size() && command[cend] != '\"')
          cend++;
        if (cend == command.size()) {
          out << "Invalid table name, expect '\"'." << std::endl;
          return true;
        }
      } else {
        while (cend < command.size() &&
               (isalpha(command[cend]) || command[cend] == '_' ||
                   isdigit(command[cend])))
          cend++;
      }
      auto table_name = command.substr(c, cend - c);
      out << "Analyzing table " << table_name << std::endl;
      Txn* txn = GetTxnManager().Begin();
      try {
        Analyze(table_name, txn->txn_id_);
        GetTxnManager().Commit(txn);
      } catch (const DBException& e) {
        err << fmt::format("DBException occurs. what(): {}\n", e.what())
            << "\n";
        GetTxnManager().Abort(txn);
        return true;
      }
      // CAUTION: TxnDLAbortException and MultiUpgradeException are not
      // catched. In future, we should provide "Connection" abstraction to
      // each instance, so CLI can also run concurrently.
      out << "Analyze completed successfully." << std::endl;
      return true;
    });

    // stats <table> Print the statistics of the table.
    cmd.SetCommand("stats", [&](std::string_view command) -> bool {
      uint32_t c = 0, cend = 0;
      while (c < command.size() && isspace(command[c]))
        c++;
      cend = c;
      if (command[c] == '\"') {
        c++;
        cend++;
        while (cend < command.size() && command[cend] != '\"')
          cend++;
        if (cend == command.size()) {
          out << "Invalid table name, expect '\"'." << std::endl;
          return true;
        }
      } else {
        while (cend < command.size() &&
               (isalpha(command[cend]) || command[cend] == '_' ||
                   isdigit(command[cend])))
          cend++;
      }
      auto table_name = command.substr(c, cend - c);
      auto stat = db_.GetTableStat(table_name);
      if (stat == nullptr) {
        out << "No stats." << std::endl;
      } else {
        auto& tab =
            db_.GetDBSchema()[db_.GetDBSchema().Find(table_name).value()];
        out << "Tuple num: " << stat->GetTupleNum() << std::endl;
        for (uint32_t i = 0;
             i < (tab.GetHidePKFlag() ? tab.GetColumns().size() - 1
                                      : tab.GetColumns().size());
             i++) {
          out << fmt::format(
              "Column {}: [Max: {}, Min: {}, Distinct rate: {}]\n",
              tab.GetColumns()[i].name_, stat->GetMax(i).ToString(),
              stat->GetMin(i).ToString(), stat->GetDistinctRate(i));
        }
        out << std::endl;
      }
      return true;
    });

    cmd.SetSQLExecutor([&](std::string_view statement) -> bool {
      StopWatch watch;
      auto ret = parser_.Parse(statement, db_.GetDBSchema());
      err << fmt::format(
          "Parsing completed in {} seconds.\n", watch.GetTimeInSeconds());
      watch.Reset();
      if (!ret.Valid()) {
        err << ret.GetErrorMsg() << std::endl;
      } else {
        Txn* txn = GetTxnManager().Begin();
        try {
          if (ret.GetPlan() == nullptr) {
            ExecuteMetadataOperation(ret, txn->txn_id_);
            if (ret.GetAST()->type_ == StatementType::CREATE_TABLE) {
              out << "Create table successfully.\n";
            } else if (ret.GetAST()->type_ == StatementType::DROP_TABLE) {
              out << "Drop table successfully.\n";
            }
          } else {
            // Query
            auto exe = GenerateExecutor(ret.GetPlan()->clone(), txn->txn_id_);
            err << fmt::format(
                "Generate executor in {} seconds.\n", watch.GetTimeInSeconds());
            auto output_schema = ret.GetPlan()->output_schema_;
            // Release unused memory
            ret.Clear();
            watch.Reset();
            auto result = GetResultFromExecutor(exe, output_schema);
            err << fmt::format(
                "Execute in {} seconds.\n", watch.GetTimeInSeconds());
            out << FormatOutputTable(result, output_schema) << std::endl;
          }
          GetTxnManager().Commit(txn);
        } catch (const DBException& e) {
          err << fmt::format("DBException occurs. what(): {}\n", e.what())
              << "\n";
          GetTxnManager().Abort(txn);
        }
        // CAUTION: TxnDLAbortException and MultiUpgradeException are not
        // catched. In future, we should provide "Connection" abstraction to
        // each instance, so CLI can also run concurrently.
      }
      return true;
    });

    out << "Welcome to Wing.\n\n";
    cmd.StartLoop();
    out << "Exiting Wing...\n";
  }

  ResultSet Execute(std::string_view statement, txn_id_t txn_id) {
    auto ret = parser_.Parse(statement, db_.GetDBSchema());
    if (!ret.Valid()) {
      DB_INFO("{}", ret.GetErrorMsg());
      return ResultSet(ret.GetErrorMsg(), "");
    } else {
      try {
        if (ret.GetPlan() == nullptr) {
          ExecuteMetadataOperation(ret, txn_id);
          return ResultSet("", "");
        } else {
          if (options_.debug_print_plan) {
            DB_INFO("statement: \n {}\nplan: \n {}", statement,
                ret.GetPlan()->ToString());
          }
          // Query
          auto exe = GenerateExecutor(ret.GetPlan()->clone(), txn_id);
          auto output_schema = ret.GetPlan()->output_schema_;
          // Release unused memory
          ret.Clear();
          auto result = GetResultFromExecutor(exe, output_schema);
          return ResultSet(std::move(result), exe->GetTotalOutputSize());
        }
      } catch (const DBException& e) {
        DB_INFO("{}", e.what());
        return ResultSet(
            "", fmt::format("DBException occurs. what(): {}\n", e.what()));
      }
    }
  }

  std::unique_ptr<PlanNode> GetPlan(std::string_view statement) {
    auto ret = parser_.Parse(statement, db_.GetDBSchema());
    if (!ret.Valid()) {
      DB_INFO("{}", ret.GetErrorMsg());
      return nullptr;
    } else {
      if (ret.GetPlan() == nullptr)
        return nullptr;
      auto plan = ret.GetPlan()->clone();
      plan = LogicalOptimizer::Optimize(std::move(plan), db_);
      plan = CostBasedOptimizer::Optimize(std::move(plan), db_);
      return plan;
    }
  }

  void SetDebugPrintPlan(bool value) { options_.debug_print_plan = value; }

  // Refresh statistics.
  void Analyze(std::string_view table_name, txn_id_t txn_id) {
    // TODO...
    throw DBException("Not implemented!");
  }

  TxnManager& GetTxnManager() { return db_.GetTxnManager(); }

 private:
  void CreateTable(const ParserResult& result, txn_id_t txn_id) {
    auto a = static_cast<const CreateTableStatement*>(result.GetAST().get());
    if (db_.GetDBSchema().Find(a->table_name_)) {
      throw DBException(
          "Create table \'{}\' error: table exists.", a->table_name_);
    }
    std::vector<ColumnSchema> columns;
    std::vector<ForeignKeySchema> fk;
    columns.reserve(a->columns_.size());
    uint32_t primary_key_index = ~0u;
    bool auto_gen_flag = false;
    bool hide_flag = false;
    for (uint32_t i = 0; auto& col : a->columns_) {
      columns.push_back(ColumnSchema{col.column_name_, col.types_, col.size_});
      if (col.is_primary_key_) {
        primary_key_index = i;
        auto_gen_flag = col.is_auto_gen_;
      }
      if (col.is_foreign_key_) {
        auto ref = db_.GetDBSchema().Find(col.ref_table_name_);
        if (!ref) {
          throw DBException(
              "Foreign key error: Referred table \'{}\' doesn't exist.",
              col.ref_table_name_);
        }
        // Referred key must be primary key.
        if (db_.GetDBSchema()[ref.value()].GetPrimaryKeySchema().name_ !=
            col.ref_column_name_) {
          throw DBException(
              "Foreign key error: Referred table \'{}\' has no primary key "
              "named \'{}\'",
              col.ref_table_name_, col.ref_column_name_);
        }
        if (db_.GetDBSchema()[ref.value()].GetPrimaryKeySchema().type_ !=
            col.types_) {
          throw DBException(
              "Foreign key error: Referred key \'{}\'.\'{}\' has a different "
              "type.",
              col.ref_table_name_, col.ref_column_name_);
        }
        if (db_.GetDBSchema()[ref.value()].GetPrimaryKeySchema().size_ !=
            col.size_) {
          throw DBException(
              "Foreign key error: Referred key \'{}\'.\'{}\' has a different "
              "size.",
              col.ref_table_name_, col.ref_column_name_);
        }
        fk.emplace_back(i, col.ref_table_name_, col.ref_column_name_,
            col.column_name_, col.types_, col.size_);
      }
      i++;
    }
    if (primary_key_index != (~0u)) {
      // Create a table to store the refcounts of the primary key.
      // The table name is __refcounts_of_XXX
      // Two columns: refcounts (int64), primary key of XXX
      auto ref_table_name = DB::GenRefTableName(a->table_name_);
      if (db_.GetDBSchema().Find(ref_table_name)) {
        throw DBException(
            "Create ref table \'{}\' error: table exists.", ref_table_name);
      }
      std::vector<ColumnSchema> col2;
      col2.push_back(
          ColumnSchema{DB::GenRefColumnName(columns[primary_key_index].name_),
              FieldType::INT64, 8});
      col2.push_back(columns[primary_key_index]);
      auto col2_clone(col2);
      db_.CreateTable(
          txn_id, TableSchema(std::move(ref_table_name), std::move(col2),
                      std::move(col2_clone), 1, false, false, {}));
    } else {
      // Create a default primary key
      // use auto_increment.
      // This primary key must be hidden.
      hide_flag = true;
      primary_key_index = columns.size();
      auto_gen_flag = true;
      columns.push_back(
          ColumnSchema{DB::GenDefaultPKName(), FieldType::INT64, 8});
    }
    auto storage_columns = columns;
    // Arrange the columns in storage.
    // In storage, VARCHAR strings are behind all other fixed fields.
    std::sort(storage_columns.begin(), storage_columns.end(),
        [&](const auto& x, const auto& y) {
          return (x.type_ == FieldType::CHAR || x.type_ == FieldType::VARCHAR) <
                 (y.type_ == FieldType::CHAR || y.type_ == FieldType::VARCHAR);
        });
    db_.CreateTable(
        txn_id, TableSchema(std::string(a->table_name_), std::move(columns),
                    std::move(storage_columns), primary_key_index,
                    auto_gen_flag, hide_flag, std::move(fk)));
  }

  void DropTable(const ParserResult& result, txn_id_t txn_id) {
    auto stmt = static_cast<const DropTableStatement*>(result.GetAST().get());
    auto index = db_.GetDBSchema().Find(stmt->table_name_);
    if (!index.has_value()) {
      throw DBException(
          "Drop table error: table \'{}\' doesn't exist.", stmt->table_name_);
    }
    auto& tab = db_.GetDBSchema()[index.value()];
    // If this table has primary key
    // Check if there are some refcounts in the ref table
    if (!tab.GetHidePKFlag()) {
      auto ret = parser_.Parse(
          fmt::format("select * from {} where {} > 0;",
              DB::GenRefTableName(stmt->table_name_),
              DB::GenRefColumnName(tab.GetPrimaryKeySchema().name_)),
          db_.GetDBSchema());
      auto exe = GenerateExecutor(ret.GetPlan()->clone(), txn_id);
      exe->Init();
      if (auto ret = exe->Next(); ret) {
        throw DBException("Drop table error: exists reference to {}={}",
            tab.GetPrimaryKeySchema().name_,
            ret.Read<StaticFieldRef>(1 * sizeof(StaticFieldRef))
                .ToString(tab.GetPrimaryKeySchema().type_,
                    tab.GetPrimaryKeySchema().size_));
      }
    }
    // If it has foreign key, then it must execute "delete from A" to decrement
    // the refcounts.
    if (tab.GetFK().size() > 0) {
      auto ret = parser_.Parse(
          fmt::format("delete from {};", stmt->table_name_), db_.GetDBSchema());
      auto exe = GenerateExecutor(ret.GetPlan()->clone(), txn_id);
      GetResultFromExecutor(exe, ret.GetPlan()->output_schema_);
    }
    // Drop the refcounts table.
    if (!tab.GetHidePKFlag()) {
      db_.DropTable(txn_id, DB::GenRefTableName(stmt->table_name_));
    }
    db_.DropTable(txn_id, stmt->table_name_);
  }

  /**
   * Execute metadata operation.
   * Metadata operation includes: create/drop table/index.
   */
  void ExecuteMetadataOperation(const ParserResult& result, txn_id_t txn_id) {
    if (result.GetAST()->type_ == StatementType::CREATE_TABLE) {
      CreateTable(result, txn_id);
    } else if (result.GetAST()->type_ == StatementType::DROP_TABLE) {
      DropTable(result, txn_id);
    }
    return;
  }

  // After this function returns, std::unique_ptr<Executor> should be released
  // immediately!! Because TupleStore in JitExecutor has been moved.
  TupleStore GetResultFromExecutor(
      std::unique_ptr<Executor>& exe, const OutputSchema& output_schema) {
    if (options_.exec_options.style == "jit") {
      exe->Init();
      auto result = const_cast<TupleStore*>(
          reinterpret_cast<const TupleStore*>(exe->Next().Data()));
      return std::move(*result);
    } else {
      exe->Init();
      auto result = GetTuplesFromNext(exe, output_schema);
      return result;
    }
  }

  // Generate executor by a logical plan.
  // This plan is released after executor is generated.
  std::unique_ptr<Executor> GenerateExecutor(
      std::unique_ptr<PlanNode> plan, txn_id_t txn_id) {
    std::unique_ptr<Executor> exe;
    plan = LogicalOptimizer::Optimize(std::move(plan), db_);
    plan = CostBasedOptimizer::Optimize(std::move(plan), db_);
    if (options_.exec_options.style == "jit") {
      exe = JitExecutorGenerator::Generate(plan.get(), db_, txn_id);
    } else {
      exe = ExecutorGenerator::Generate(plan.get(), db_, txn_id);
    }
    return exe;
  }

  /**
   * Collect all the tuples from executor.
   * For jit executor, the result is returned in one call.
   * For normal executors, the result is returned by returning a tuple at a
   * time.
   */
  TupleStore GetTuplesFromNext(
      std::unique_ptr<Executor>& exe, const OutputSchema& schema) {
    TupleStore ret(schema);
    auto result = exe->Next();
    while (result) {
      ret.Append(result.Data());
      result = exe->Next();
    }
    return ret;
  }

  /**
   * Format the output as follows:
   * +------------+----+
   * | column name|   a|
   * +------------+----+
   * |      114514|  xx|
   * +------------+----+
   * If the number of lines exceeds 100, then it prints "..." instead.
   */
  std::string FormatOutputTable(const TupleStore& output,
      const OutputSchema& schema, uint32_t limit = 100) {
    std::vector<uint8_t*> vec;
    if (output.GetPointerVec().size() > limit) {
      for (uint32_t i = 0; i < limit / 2; i++) {
        vec.push_back(output.GetPointerVec()[i]);
      }
      for (uint32_t i = output.GetPointerVec().size() - limit / 2;
           i < output.GetPointerVec().size(); i++) {
        vec.push_back(output.GetPointerVec()[i]);
      }
    } else {
      vec = output.GetPointerVec();
    }
    std::vector<uint32_t> lengths(schema.size());
    for (uint32_t i = 0; i < schema.size(); i++) {
      lengths[i] = schema[i].column_name_.size();
      for (auto& a : vec) {
        auto str = SingleTuple(a)
                       .Read<StaticFieldRef>(i * sizeof(StaticFieldRef))
                       .ToString(schema[i].type_, schema[i].size_);
        lengths[i] = std::max(lengths[i], (uint32_t)str.length());
      }
      lengths[i] += 2;
      if (lengths[i] < 4) {
        lengths[i] = 4;
      }
    }
    std::string ret;
    for (auto L : lengths) {
      ret += '+' + std::string(L, '-');
    }
    ret += "+\n";
    for (uint32_t j = 0; j < schema.size(); ++j) {
      ret += '|';
      ret += std::string(lengths[j] - schema[j].column_name_.length(), ' ') +
             schema[j].column_name_;
    }
    ret += "|\n";
    for (auto L : lengths) {
      ret += '+' + std::string(L, '-');
    }
    ret += "+\n";
    for (uint32_t i = 0; i < vec.size(); i++) {
      if (i == limit / 2 && output.GetPointerVec().size() > limit) {
        // Generate ....| ....| ....|
        for (uint32_t j = 0; j < schema.size(); j++) {
          ret += "|" + std::string(lengths[j] - 3, ' ') + std::string(3, '.');
        }
        ret += "|\n";
      }
      for (uint32_t j = 0; j < schema.size(); j++) {
        auto str = SingleTuple(vec[i])
                       .Read<StaticFieldRef>(j * sizeof(StaticFieldRef))
                       .ToString(schema[j].type_, schema[j].size_);
        ret += "|" + std::string(lengths[j] - str.length(), ' ') + str;
      }
      ret += "|\n";
    }
    for (auto L : lengths) {
      ret += '+' + std::string(L, '-');
    }
    ret += "+\n";
    ret += fmt::format("{} rows in total.", output.GetPointerVec().size());
    return ret;
  }
  WingOptions options_;
  DB db_;
  Parser parser_;
};

Instance::Instance(std::string_view db_file, WingOptions options) {
  ptr_ = std::make_unique<Impl>(db_file, options);
}
Instance::~Instance() {}
ResultSet Instance::Execute(std::string_view statement) {
  Txn* txn = ptr_->GetTxnManager().Begin();
  auto res = ptr_->Execute(statement, txn->txn_id_);
  ptr_->GetTxnManager().Commit(txn);
  return res;
}
ResultSet Instance::Execute(std::string_view statement, txn_id_t txn_id) {
  return ptr_->Execute(statement, txn_id);
}
void Instance::ExecuteShell() { ptr_->ExecuteShell(); }
void Instance::Analyze(std::string_view table_name) {
  Txn* txn = ptr_->GetTxnManager().Begin();
  ptr_->Analyze(table_name, txn->txn_id_);
  ptr_->GetTxnManager().Commit(txn);
}
std::unique_ptr<PlanNode> Instance::GetPlan(std::string_view statement) {
  return ptr_->GetPlan(statement);
}

void Instance::SetDebugPrintPlan(bool value) { ptr_->SetDebugPrintPlan(value); }

TxnManager& Instance::GetTxnManager() { return ptr_->GetTxnManager(); }

}  // namespace wing

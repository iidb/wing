#include "execution/executor.hpp"

#include "catalog/schema.hpp"
#include "execution/delete_executor.hpp"
#include "execution/filter_executor.hpp"
#include "execution/insert_executor.hpp"
#include "execution/print_executor.hpp"
#include "execution/project_executor.hpp"
#include "execution/seqscan_executor.hpp"

namespace wing {

std::unique_ptr<Executor> ExecutorGenerator::Generate(
    const PlanNode* plan, DB& db, size_t txn_id) {
  if (plan == nullptr) {
    throw DBException("Invalid PlanNode.");
  }

  else if (plan->type_ == PlanType::Project) {
    auto project_plan = static_cast<const ProjectPlanNode*>(plan);
    return std::make_unique<ProjectExecutor>(project_plan->output_exprs_,
        project_plan->ch_->output_schema_,
        Generate(project_plan->ch_.get(), db, txn_id));
  }

  else if (plan->type_ == PlanType::Filter) {
    auto filter_plan = static_cast<const FilterPlanNode*>(plan);
    return std::make_unique<FilterExecutor>(filter_plan->predicate_.GenExpr(),
        filter_plan->ch_->output_schema_,
        Generate(filter_plan->ch_.get(), db, txn_id));
  }

  else if (plan->type_ == PlanType::Print) {
    auto print_plan = static_cast<const PrintPlanNode*>(plan);
    return std::make_unique<PrintExecutor>(
        print_plan->values_, print_plan->num_fields_per_tuple_);
  }

  else if (plan->type_ == PlanType::Insert) {
    auto insert_plan = static_cast<const InsertPlanNode*>(plan);
    auto table_schema_index = db.GetDBSchema().Find(insert_plan->table_name_);
    if (!table_schema_index) {
      throw DBException("Cannot find table \'{}\'", insert_plan->table_name_);
    }
    auto& tab = db.GetDBSchema()[table_schema_index.value()];
    auto gen_pk = tab.GetAutoGenFlag()
                      ? db.GetGenPKHandle(txn_id, tab.GetName())
                      : nullptr;
    return std::make_unique<InsertExecutor>(
        db.GetModifyHandle(txn_id, insert_plan->table_name_),
        Generate(insert_plan->ch_.get(), db, txn_id),
        FKChecker(tab.GetFK(), tab, txn_id, db), gen_pk, tab);
  }

  else if (plan->type_ == PlanType::SeqScan) {
    auto seqscan_plan = static_cast<const SeqScanPlanNode*>(plan);
    auto table_schema_index = db.GetDBSchema().Find(seqscan_plan->table_name_);
    if (!table_schema_index) {
      throw DBException("Cannot find table \'{}\'", seqscan_plan->table_name_);
    }
    return std::make_unique<SeqScanExecutor>(
        db.GetIterator(txn_id, seqscan_plan->table_name_),
        seqscan_plan->predicate_.GenExpr(), seqscan_plan->output_schema_);
  }

  else if (plan->type_ == PlanType::Delete) {
    auto delete_plan = static_cast<const DeletePlanNode*>(plan);
    auto table_schema_index = db.GetDBSchema().Find(delete_plan->table_name_);
    if (!table_schema_index) {
      throw DBException("Cannot find table \'{}\'", delete_plan->table_name_);
    }
    auto& tab = db.GetDBSchema()[table_schema_index.value()];
    return std::make_unique<DeleteExecutor>(
        db.GetModifyHandle(txn_id, delete_plan->table_name_),
        Generate(delete_plan->ch_.get(), db, txn_id),
        FKChecker(tab.GetFK(), tab, txn_id, db),
        PKChecker(tab.GetName(), tab.GetHidePKFlag(), txn_id, db), tab);
  }

  throw DBException("Unsupported plan node.");
}

}  // namespace wing
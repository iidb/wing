#pragma once

#include "execution/executor.hpp"

namespace wing {

class SeqScanVecExecutor : public VecExecutor {
 public:
  SeqScanVecExecutor(const ExecOptions& options,
      std::unique_ptr<Iterator<const uint8_t*>> iter,
      const std::unique_ptr<Expr>& predicate, const OutputSchema& input_schema,
      const TableSchema& table_schema)
    : VecExecutor(options),
      iter_(std::move(iter)),
      predicate_(predicate.get(), input_schema),
      schema_(input_schema),
      table_schema_(table_schema) {}
  void Init() override {
    iter_->Init();
    result_.resize(schema_.GetCols().size());
    tuples_.Init(schema_.GetTypes(), max_batch_size_);
  }
  TupleBatch InternalNext() override {
    auto ret = iter_->Next();
    tuples_.Clear();
    while (ret) {
      Tuple::DeSerialize(
          result_.data(), ret, table_schema_.GetStorageColumns());
      if (!predicate_ || predicate_.Evaluate(result_.data()).ReadInt() != 0) {
        tuples_.Append(result_);
        if (tuples_.IsFull()) {
          break;
        }
      }
      ret = iter_->Next();
    }
    return tuples_;
  }

 private:
  std::unique_ptr<Iterator<const uint8_t*>> iter_;
  ExprExecutor predicate_;
  OutputSchema schema_;
  TableSchema table_schema_;

  std::vector<StaticFieldRef> result_;
  TupleBatch tuples_;
};

}  // namespace wing

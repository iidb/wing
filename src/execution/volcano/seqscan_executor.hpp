#pragma once

#include "execution/executor.hpp"

namespace wing {

class SeqScanExecutor : public Executor {
 public:
  SeqScanExecutor(std::unique_ptr<Iterator<const uint8_t*>> iter,
      const std::unique_ptr<Expr>& predicate, const OutputSchema& input_schema,
      const TableSchema& table_schema)
    : iter_(std::move(iter)),
      predicate_(predicate.get(), input_schema),
      schema_(input_schema),
      table_schema_(table_schema) {}
  void Init() override {
    iter_->Init();
    result_.resize(schema_.GetCols().size());
  }
  SingleTuple Next() override {
    auto ret = iter_->Next();
    while (ret) {
      Tuple::DeSerialize(
          result_.data(), ret, table_schema_.GetStorageColumns());
      if (!predicate_ || predicate_.Evaluate(result_.data()).ReadInt() != 0) {
        break;
      }
      ret = iter_->Next();
    }
    if (ret) {
      return result_;
    } else {
      return {};
    }
  }

 private:
  std::unique_ptr<Iterator<const uint8_t*>> iter_;
  ExprExecutor predicate_;
  OutputSchema schema_;
  TableSchema table_schema_;

  std::vector<StaticFieldRef> result_;
};

}  // namespace wing

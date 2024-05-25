#pragma once

#include "execution/executor.hpp"

namespace wing {

class SeqScanVecExecutor : public VecExecutor {
 public:
  SeqScanVecExecutor(const ExecOptions& options,
      std::unique_ptr<Iterator<const uint8_t*>> iter,
      const std::unique_ptr<Expr>& predicate,
      std::shared_ptr<BitVector> valid_bits, const OutputSchema& input_schema,
      const TableSchema& table_schema)
    : VecExecutor(options),
      iter_(std::move(iter)),
      predicate_(predicate.get(), input_schema),
      valid_bits_(valid_bits),
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
        if (valid_bits_ && valid_bits_index_ < valid_bits_->size() &&
            !(*valid_bits_)[valid_bits_index_]) {
          valid_bits_index_ += 1;
        } else {
          valid_bits_index_ += 1;
          tuples_.Append(result_);
          if (tuples_.IsFull()) {
            break;
          }
        }
      }
      ret = iter_->Next();
    }
    return tuples_;
  }

  virtual size_t GetTotalOutputSize() const override {
    return stat_output_size_;
  }

 private:
  std::unique_ptr<Iterator<const uint8_t*>> iter_;
  ExprExecutor predicate_;
  std::shared_ptr<BitVector> valid_bits_;
  size_t valid_bits_index_{0};
  OutputSchema schema_;
  TableSchema table_schema_;

  std::vector<StaticFieldRef> result_;
  TupleBatch tuples_;
};

}  // namespace wing

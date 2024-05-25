#pragma once

#include "execution/executor.hpp"

namespace wing {

class OutputVecExecutor : public Executor {
 public:
  OutputVecExecutor(
      std::unique_ptr<VecExecutor> ch, const OutputSchema& input_schema)
    : ch_(std::move(ch)), schema_(input_schema) {}
  void Init() override {
    offset_ = 0;
    result_.resize(schema_.size());
    ch_->Init();
  }
  SingleTuple Next() override {
    while (true) {
      if (offset_ == tuples_.size()) {
        tuples_ = ch_->Next();
        offset_ = 0;
        if (tuples_.size() == 0) {
          return {};
        }
      }
      while (offset_ < tuples_.size() && !tuples_.IsValid(offset_)) {
        offset_ += 1;
      }
      if (offset_ < tuples_.size()) {
        for (uint32_t i = 0; i < result_.size(); i++) {
          result_[i] = tuples_.Get(offset_, i);
        }
        offset_ += 1;
        return result_;
      } else {
        continue;
      }
    }
    return {};
  }

  size_t GetTotalOutputSize() const override {
    return ch_->GetTotalOutputSize();
  }

 private:
  std::unique_ptr<VecExecutor> ch_;
  OutputSchema schema_;
  size_t offset_{0};
  TupleBatch tuples_;

  std::vector<StaticFieldRef> result_;
};

}  // namespace wing

#pragma once

#include "execution/executor.hpp"
#include "execution/predicate_transfer/pt_reducer.hpp"

namespace wing {

class PtVecExecutor : public VecExecutor {
 public:
  PtVecExecutor(const ExecOptions& exec_options,
      std::unique_ptr<VecExecutor> ch, std::unique_ptr<PtReducer> reducer)
    : VecExecutor(exec_options),
      ch_(std::move(ch)),
      reducer_(std::move(reducer)) {}

  void Init() override { ch_->Init(); }

  TupleBatch InternalNext() override {
    if (!init_flag_) {
      init_flag_ = true;
      reducer_->Execute();
    }
    return ch_->Next();
  }

  virtual size_t GetTotalOutputSize() const override {
    return ch_->GetTotalOutputSize();
  }

 private:
  std::unique_ptr<VecExecutor> ch_;
  std::unique_ptr<PtReducer> reducer_;
  bool init_flag_{false};
};

}  // namespace wing

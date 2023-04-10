#ifndef SAKURA_CMD_LINE_H__
#define SAKURA_CMD_LINE_H__

#include <functional>
#include <memory>
#include <string>

namespace wing {

// SQLCmdLine tool using replxx.
class SQLCmdLine {
 public:
  using CallBackFuncType = std::function<bool(std::string_view)>;
  SQLCmdLine();
  ~SQLCmdLine();
  // If the beginning is cmd, then func_ will be invoked.
  // It can only be one line.
  void SetCommand(std::string_view cmd, CallBackFuncType&& func_);
  // Set executor for SQL. This will continue reading until the last character
  // is ';'.
  void SetSQLExecutor(CallBackFuncType&& func_);
  void StartLoop();

 private:
  class Impl;
  std::unique_ptr<Impl> ptr_;
};
}  // namespace wing

#endif
#include "common/cmdline.hpp"

#include <iostream>
#include <map>

#include "replxx.hxx"

namespace wing {
class SQLCmdLine::Impl {
 public:
  Impl() {
    // Copy from https://github.com/AmokHuginnsson/replxx/blob/master/examples/cxx-api.cxx
    rx_.install_window_change_handler();
    // set the max history size
    rx_.set_max_history_size(128);

    // set the max number of hint rows to show
    rx_.set_max_hint_rows(3);
    rx_.set_word_break_characters(" \n\t.,-%!;:=*~^'\"/?<>|[](){}");
  }
  ~Impl() {}
  void SetCommand(std::string_view cmd, std::function<bool(std::string_view)>&& func_) { funcs_.push_back({std::string(cmd), std::move(func_)}); }
  void SetSQLExecutor(std::function<bool(std::string_view)>&& func_) { sql_func_ = std::move(func_); }
  void StartLoop() {
    std::string prompt = "wing> ";
    std::string wait = "...   ";
    while (true) {
      const char* cinput;
      do {
        cinput = rx_.input(prompt);
      } while ((cinput == nullptr) && (errno == EAGAIN));
      if (cinput == nullptr) {
        continue;
      }
      std::string_view input(cinput);
      if (input.empty()) {
        // Press enter.
        continue;
      }
      rx_.history_add(cinput);
      size_t pos = 0;
      while (pos < input.size() && isspace(input[pos])) pos++;
      if (pos == input.size()) {
        continue;
      }
      bool flag = false;
      for (auto& [cmd, func] : funcs_)
        if (input.substr(pos, cmd.length()) == cmd) {
          if (!func(input.substr(pos + cmd.length(), input.length() - pos - cmd.length()))) {
            return;
          } else {
            flag = true;
            break;
          }
        }
      if (flag) {
        continue;
      }
      // Execute multiple SQL statments.
      std::string stmts(input);
      // Read statements.
      while (true) {
        if (input.back() == ';') {
          break;
        }
        do {
          cinput = rx_.input(wait);
        } while ((cinput == nullptr) && (errno == EAGAIN));
        if (cinput == nullptr) {
          continue;
        }
        input = std::string_view(cinput);
        if (input.empty()) {
          // Press enter.
          continue;
        }
        rx_.history_add(cinput);
        stmts += input;
      }
      // Execute statments.
      size_t stmt_begin = 0;
      while (true) {
        auto stmt_end = stmts.find(';', stmt_begin);
        if (stmt_end == std::string::npos) break;
        if (!sql_func_(std::string_view(stmts.begin() + stmt_begin, stmts.begin() + stmt_end + 1))) {
          return;
        }
        stmt_begin = stmt_end + 1;
      }
    }
  }

 private:
  std::vector<std::pair<std::string, CallBackFuncType>> funcs_;
  CallBackFuncType sql_func_;
  replxx::Replxx rx_;
};

SQLCmdLine::SQLCmdLine() { ptr_ = std::make_unique<Impl>(); }
SQLCmdLine::~SQLCmdLine() {}

void SQLCmdLine::SetCommand(std::string_view cmd, CallBackFuncType&& func_) { ptr_->SetCommand(cmd, std::move(func_)); }
void SQLCmdLine::SetSQLExecutor(CallBackFuncType&& func_) { ptr_->SetSQLExecutor(std::move(func_)); }
void SQLCmdLine::StartLoop() { ptr_->StartLoop(); }

}  // namespace wing
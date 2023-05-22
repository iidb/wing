#ifndef SAKURA_EXCEPTION_H__
#define SAKURA_EXCEPTION_H__
#include <execinfo.h>

#include <exception>
#include <string>

#include "fmt/core.h"

namespace wing {

// This exception is raised when there are some errors in SQL.
// For example foreign key error, duplicate key error and so on.
// Or transaction aborts because of invalid inputs.
class DBException : public std::exception {
 protected:
  std::string errmsg_;

 public:
  template <typename... T>
  DBException(fmt::string_view format, T&&... errmsg)
    : errmsg_(fmt::vformat(
          format, fmt::make_format_args(std::forward<T>(errmsg)...))) {}
  const char* what() const noexcept { return errmsg_.c_str(); }
};

// Abort because of invalid behavior: e.g., violate 2PL, release a lock that
// has not been acquired.
class TxnInvalidBehaviorException : public DBException {
 public:
  template <typename... T>
  TxnInvalidBehaviorException(fmt::string_view format, T&&... errmsg)
    : DBException(format, std::forward<T>(errmsg)...) {}

  const char* what() const noexcept { return errmsg_.c_str(); }
};

// Abort because of another txn is upgrading. Should self-abort and rerun.
class MultiUpgradeException : public std::exception {
  std::string errmsg_;

 public:
  template <typename... T>
  MultiUpgradeException(fmt::string_view format, T&&... errmsg)
    : errmsg_(fmt::vformat(
          format, fmt::make_format_args(std::forward<T>(errmsg)...))) {}
  const char* what() const noexcept { return errmsg_.c_str(); }
};

// Abort because of deadlock. e.g., self-abort in wait-die.
class TxnDLAbortException : public std::exception {
  std::string errmsg_;

 public:
  template <typename... T>
  TxnDLAbortException(fmt::string_view format, T&&... errmsg)
    : errmsg_(fmt::vformat(
          format, fmt::make_format_args(std::forward<T>(errmsg)...))) {}
  const char* what() const noexcept { return errmsg_.c_str(); }
};

}  // namespace wing

#endif
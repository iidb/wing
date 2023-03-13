#ifndef SAKURA_EXCEPTION_H__
#define SAKURA_EXCEPTION_H__

#include <exception>
#include <string>

#include "fmt/core.h"

namespace wing {

// This exception is raised when there are some errors in SQL.
// For example foreign key error, duplicate key error and so on.
// Or transaction aborts.
class DBException : public std::exception {
  std::string errmsg_;

 public:
  template <typename... T>
  DBException(fmt::string_view format, T&&... errmsg) : errmsg_(fmt::vformat(format, fmt::make_format_args(std::forward<T>(errmsg)...))) {}
  const char* what() const noexcept { return errmsg_.c_str(); }
};

}  // namespace wing

#endif
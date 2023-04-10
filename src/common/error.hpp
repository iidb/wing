#ifndef SAKURA_ERROR_H_
#define SAKURA_ERROR_H_

#include <memory>
#include <ostream>
#include <variant>

#define panic(...)                                                      \
  do {                                                                  \
    fprintf(stderr, "panic: %s:%u: %s:", __FILE__, __LINE__, __func__); \
    fprintf(stderr, " " __VA_ARGS__);                                   \
    abort();                                                            \
  } while (0)

#define crash_if(cond, ...) \
  do {                      \
    if (cond) {             \
      panic(__VA_ARGS__);   \
    }                       \
  } while (0)

namespace wing {
namespace io {
class Error;
}  // namespace io

namespace error {
class Error {
 public:
  std::string_view description() { return description_; }

 private:
  Error(std::string&& description) : description_(std::move(description)) {}
  std::string description_;
  friend io::Error;
};
}  // namespace error

template <typename T, typename E>
using Result = std::variant<T, E>;

// Return if error
#define EXTRACT_RESULT(arg)                                  \
  ({                                                         \
    decltype(arg) result = std::forward<decltype(arg)>(arg); \
    if (result.index() == 1)                                 \
      return std::move(std::get<1>(result));                 \
    std::move(std::get<0>(result));                          \
  })

namespace io {
enum class ErrorKind {
  NotFound,
  AlreadyExists,
  Other,
};

class Error {
 public:
  Error(const Error&) = delete;
  Error& operator=(const Error&) = delete;
  Error(Error&& e) : kind_(e.kind_), error_(std::move(e.error_)) {}
  Error& operator=(Error&& e) {
    kind_ = e.kind_;
    error_ = std::move(e.error_);
    return *this;
  }
  static Error New(ErrorKind kind, const char* error) {
    return Error(kind,
        std::unique_ptr<error::Error>(new error::Error(std::string(error))));
  }
  static Error New(ErrorKind kind, std::string&& error) {
    return Error(kind,
        std::unique_ptr<error::Error>(new error::Error(std::move(error))));
  }
  static Error from(ErrorKind kind) { return Error(kind, nullptr); }
  ErrorKind kind() const { return kind_; }
  std::string to_string() const;

 private:
  Error(ErrorKind kind, std::unique_ptr<error::Error> error)
    : kind_(kind), error_(std::move(error)) {}
  ErrorKind kind_;
  std::unique_ptr<error::Error> error_;
  friend std::ostream& operator<<(std::ostream& out, const Error& e);
};

std::ostream& operator<<(std::ostream& out, const Error& e);

}  // namespace io
}  // namespace wing

#endif  // SAKURA_ERROR_H_

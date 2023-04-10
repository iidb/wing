#include "common/error.hpp"

#include <sstream>

namespace wing {
namespace io {
std::string Error::to_string() const {
  std::ostringstream out;
  out << *this;
  return std::move(out).str();
}
std::ostream& operator<<(std::ostream& out, const Error& e) {
  if (e.error_ == nullptr) {
    switch (e.kind()) {
      case ErrorKind::NotFound:
        out << "entity not found";
        break;
      case ErrorKind::AlreadyExists:
        out << "entity already exists";
      case ErrorKind::Other:
        out << "other error";
    }
  } else {
    out << e.error_->description();
  }
  return out;
}
}  // namespace io
}  // namespace wing

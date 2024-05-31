#include <cpptrace/cpptrace.hpp>

#include "common/printstack.hpp"

namespace wing {


std::string get_stack_trace() {
  return cpptrace::generate_trace().to_string();
}

}
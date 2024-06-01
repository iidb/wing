
#ifdef USE_CPPTRACE
#include <cpptrace/cpptrace.hpp>
#endif

#include "common/printstack.hpp"

namespace wing {

std::string get_stack_trace() {
#ifdef USE_CPPTRACE
  return cpptrace::generate_trace().to_string();
#else
  return "";
#endif
}

}  // namespace wing

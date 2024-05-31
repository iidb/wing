#include "gtest/gtest.h"
#include "common/printstack.hpp"

#include <signal.h>

void handler(int sig) {
  ::signal(sig, SIG_DFL); // exit normally
  std::cout << wing::get_stack_trace() << std::endl;
  ::raise(sig);
}

int main(int argc, char *argv[]) {
  signal(SIGSEGV, &handler);
  signal(SIGABRT, &handler);
  // https://github.com/google/googletest/blob/main/docs/advanced.md#global-set-up-and-tear-down
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

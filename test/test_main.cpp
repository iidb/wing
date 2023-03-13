#include "gtest/gtest.h"

int main(int argc, char *argv[]) {
  // https://github.com/google/googletest/blob/main/docs/advanced.md#global-set-up-and-tear-down
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
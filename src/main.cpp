#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

#include "common/logging.hpp"
#include "instance/instance.hpp"

int main(int argc, char** argv) {
  bool use_jit_flag_ = false;
  for (int i = 2; i < argc; i++) {
    // Use JIT.
    if (strcmp(argv[i], "--jit") == 0) {
      use_jit_flag_ = true;
    }
    // Create a new empty DB.
    else if (strcmp(argv[i], "--new") == 0) {
      std::filesystem::remove(argv[1]);
    } else {
      std::cerr << "Unrecognized cmdline option: " << argv[i] << std::endl;
      return -1;
    }
  }
  if (argc < 2) {
    std::cerr << "Expect file name." << std::endl;
    return -1;
  }
  if (argv[1][0] == '-' || argv[1][0] == '\0') {
    std::cerr << "Warning: please check your file name." << std::endl;
    std::cerr << fmt::format("Your file name is {}", argv[1]) << std::endl;
  }
  auto db = std::make_unique<wing::Instance>(argv[1], use_jit_flag_);
  db->ExecuteShell();
}
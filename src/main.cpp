#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <signal.h>

#include "catalog/options.hpp"
#include "common/logging.hpp"
#include "instance/instance.hpp"
#include "common/printstack.hpp"

void handler(int sig) {
  ::signal(sig, SIG_DFL); // exit normally
  std::cout << wing::get_stack_trace() << std::endl;
}

int main(int argc, char** argv) {
  signal(SIGSEGV, &handler);
  wing::WingOptions options;
  for (int i = 2; i < argc; i++) {
    // Use JIT.
    if (strcmp(argv[i], "--jit") == 0) {
      options.exec_options.style = "jit";
    } else if (strcmp(argv[i], "--vec") == 0) {
      options.exec_options.style = "vec";
    } else if (strcmp(argv[i], "--volcano") == 0) {
      options.exec_options.style = "volcano";
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
  auto db = std::make_unique<wing::Instance>(argv[1], options);
  db->ExecuteShell();
}

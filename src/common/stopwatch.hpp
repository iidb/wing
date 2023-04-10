#ifndef SAKURA_STOPWATCH_H__
#define SAKURA_STOPWATCH_H__

#include <chrono>

namespace wing {
// convenient timing tool
// Usage:
// StopWatch sw;
// ...some functions...
// std::cout << "Use " << sw.GetTimeInSeconds() << " s" << std::endl;
// You can also reset it.
class StopWatch {
 public:
  StopWatch() { start_ = std::chrono::system_clock::now(); }
  double GetTimeInSeconds() {
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> diff = end - start_;
    return diff.count();
  }
  void Reset() { start_ = std::chrono::system_clock::now(); }

 private:
  std::chrono::time_point<std::chrono::system_clock> start_;
};

}  // namespace wing

#endif
#ifndef SAKURA_LOCK_MODE_H__
#define SAKURA_LOCK_MODE_H__
#include <fmt/format.h>

namespace wing {
enum class LockMode { S, X, IS, IX, SIX };
}

template <>
struct fmt::formatter<wing::LockMode> : formatter<std::string> {
  // "parse" is a compile-time function that extracts the presentation format
  // from the format string
  template <typename ParseContext>
  auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  // "format" is called by the formatting library to format the arguments
  template <typename FormatContext>
  auto format(const wing::LockMode& mode, FormatContext& ctx) {
    switch (mode) {
      case wing::LockMode::S:
        return formatter<std::string>::format("S", ctx);
      case wing::LockMode::X:
        return formatter<std::string>::format("X", ctx);
      case wing::LockMode::IS:
        return formatter<std::string>::format("IS", ctx);
      case wing::LockMode::IX:
        return formatter<std::string>::format("IX", ctx);
      case wing::LockMode::SIX:
        return formatter<std::string>::format("SIX", ctx);
      default:
        return formatter<std::string>::format("Invalid", ctx);
    }
  }
};

#endif
#pragma once

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <ostream>
#include <string_view>
#include <vector>

#define panic(...)                                                         \
  do {                                                                     \
    /* The "if" should be optimized out by the compiler */                 \
    if (sizeof(#__VA_ARGS__) == 1) {                                       \
      fprintf(stderr, "panic: %s:%u: %s\n", __FILE__, __LINE__, __func__); \
    } else {                                                               \
      fprintf(stderr, "panic: %s:%u: %s", __FILE__, __LINE__, __func__);   \
      fprintf(stderr, ": " __VA_ARGS__);                                   \
      fprintf(stderr, "\n");                                               \
    }                                                                      \
    abort();                                                               \
  } while (0)

#define wing_assert(cond, ...)                             \
  do {                                                     \
    if (!(cond)) {                                         \
      panic("Assertion `" #cond "` failed. " __VA_ARGS__); \
    }                                                      \
  } while (0)

#define __first_arg(a, ...) a
#define __drop_first_arg_with_prepended_comma(a, ...) , ##__VA_ARGS__

#define wing_assert_eq(l, r, ...)                                       \
  do {                                                                  \
    auto left = (l);                                                    \
    auto right = (r);                                                   \
    if (sizeof(#__VA_ARGS__) == 1) {                                    \
      wing_assert(left == right, "left = %s, right = %s",               \
          std::to_string(left).c_str(), std::to_string(right).c_str()); \
    } else {                                                            \
      wing_assert(left == right,                                        \
          "left = %s, right = %s. " __first_arg(__VA_ARGS__),           \
          std::to_string(left).c_str(),                                 \
          std::to_string(right).c_str()                                 \
              __drop_first_arg_with_prepended_comma(__VA_ARGS__));      \
    }                                                                   \
  } while (0)

#define wing_assert_ne(l, r, ...)                                       \
  do {                                                                  \
    auto left = (l);                                                    \
    auto right = (r);                                                   \
    if (sizeof(#__VA_ARGS__) == 1) {                                    \
      wing_assert(left != right, "left = %s, right = %s",               \
          std::to_string(left).c_str(), std::to_string(right).c_str()); \
    } else {                                                            \
      wing_assert(left != right,                                        \
          "left = %s, right = %s. " __first_arg(__VA_ARGS__),           \
          std::to_string(left).c_str(),                                 \
          std::to_string(right).c_str()                                 \
              __drop_first_arg_with_prepended_comma(__VA_ARGS__));      \
    }                                                                   \
  } while (0)

cmake_minimum_required(VERSION 3.2)
project(wing)
set(CMAKE_CXX_STANDARD 20)

# Set default build type to Release
IF (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING
        "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel." FORCE)
ENDIF()

option(SAKURA_ONLINE_JUDGE "The online judge will set this to ON" OFF)

if (SAKURA_ONLINE_JUDGE)
  add_compile_definitions(SAKURA_ONLINE_JUDGE)
endif()

# Submodules
add_subdirectory(third_party/fmt)
add_subdirectory(third_party/replxx)

# Source files
file(GLOB DB_SOURCE 
  src/common/*
  src/execution/*
  src/instance/*
  src/catalog/*
  src/parser/*
  src/storage/*
  src/type/*
  src/plan/*
)

# Configure LLVM
if (BUILD_JIT)
  find_package(LLVM REQUIRED CONFIG)
  message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
  message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
  set(LLVM_LINK_COMPONENTS
    Analysis
    Core
    ExecutionEngine
    InstCombine
    Object
    OrcJIT
    RuntimeDyld
    ScalarOpts
    Support
    TransformUtils
    native)
  llvm_map_components_to_libnames(llvm_libs ${LLVM_LINK_COMPONENTS})
  include_directories(${LLVM_INCLUDE_DIRS})
  add_definitions(${LLVM_DEFINITIONS})
  add_compile_definitions(BUILD_JIT=1)
  file(GLOB DB_JIT_SOURCE src/jit/*)
  set(DB_SOURCE ${DB_SOURCE} ${DB_JIT_SOURCE})
endif()

add_compile_options("-Wall" "-march=native")

# Address Sanitizer can only be enabled in Debug mode
option(ASAN "Use Address Sanitizer" OFF)
if (ASAN)
  string( TOLOWER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_LOWER )
  if (CMAKE_BUILD_TYPE_LOWER STREQUAL "debug")
    add_compile_options("-fsanitize=address" "-fno-omit-frame-pointer")
    add_link_options("-fsanitize=address" "-fno-omit-frame-pointer")
    message(STATUS "asan is enabled.")
  else()
    message(STATUS "asan will not be enabled in modes other than Debug.")
  endif()
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND MINGW)
  # https://stackoverflow.com/questions/31617034/stdfuture-and-clang-with-stdlib-libstdc
  # If you are using Clang but library was compiled by GCC
  add_compile_options("-femulated-tls")
endif()

add_library(wing_lib STATIC ${DB_SOURCE})
add_executable(${PROJECT_NAME} src/main.cpp)

# Include directories
target_include_directories(${PROJECT_NAME} PRIVATE src third_party/fmt)
target_include_directories(wing_lib PRIVATE src third_party/replxx/include third_party/fmt)
target_link_libraries(wing_lib PRIVATE ${llvm_libs} fmt replxx)
target_link_libraries(wing_lib PUBLIC pthread)
target_link_libraries(${PROJECT_NAME} PRIVATE wing_lib fmt)

# Test
add_subdirectory(test)

set(P1_FILES
  "src/catalog/db.cpp"
  "src/storage/bplus-tree.cpp"
  "src/storage/bplus-tree.hpp"
  "src/storage/page-manager.cpp"
  "src/storage/page-manager.hpp"
)

add_custom_target(submit-p1
  zip ${CMAKE_CURRENT_BINARY_DIR}/p1-submission.zip ${P1_FILES}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

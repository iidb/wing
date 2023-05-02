add_executable(
  test_basic
  test_main.cpp
  test_basic.cpp
)

add_executable(
  test_btree
  test_main.cpp
  bplus-tree.cpp
)

add_executable(
  test_exec
  test_main.cpp
  test_exec.cpp
)

add_executable(
  test_opm
  test_main.cpp
  test_opm.cpp
)

add_executable(
  test_job
  test_main.cpp
  test_job.cpp
)

target_include_directories(test_basic PRIVATE ../src ../third_party/fmt)
target_include_directories(test_btree PRIVATE ../src ../third_party/fmt)
target_include_directories(test_exec PRIVATE ../src ../third_party/fmt)
target_include_directories(test_opm PRIVATE ../src ../third_party/fmt)
target_include_directories(test_job PRIVATE ../src ../third_party/fmt)

find_library(GTEST_LIB gtest)
if (NOT GTEST_LIB)
  if (POLICY CMP0057)
    # Operator IN_LIST in FetchContent
    cmake_policy(SET CMP0057 NEW)
  endif()

  if (POLICY CMP0135)
    # The DOWNLOAD_EXTRACT_TIMESTAMP option
    cmake_policy(SET CMP0135 NEW)
  endif()

  include(FetchContent)
  # GoogleTest
  # https://google.github.io/googletest/quickstart-cmake.html
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.13.0.zip
  )
  # For Windows: Prevent overriding the parent project's compiler/linker settings
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
  enable_testing()

  target_link_libraries(test_basic wing_lib GTest::gmock fmt)
  target_link_libraries(test_btree wing_lib GTest::gmock fmt)
  target_link_libraries(test_exec wing_lib GTest::gmock fmt)
  target_link_libraries(test_opm wing_lib GTest::gmock fmt)
  target_link_libraries(test_job wing_lib GTest::gmock fmt)
else()
  target_link_libraries(test_basic wing_lib fmt gtest)
  target_link_libraries(test_btree wing_lib fmt gtest)
  target_link_libraries(test_exec wing_lib fmt gtest)
  target_link_libraries(test_opm wing_lib fmt gtest)
  target_link_libraries(test_job wing_lib fmt gtest)
endif()

include(GoogleTest)
gtest_discover_tests(test_basic)
gtest_discover_tests(test_btree)
gtest_discover_tests(test_exec)
gtest_discover_tests(test_opm)
gtest_discover_tests(test_job)

# Configure other files
configure_file(imdb_job/create_table.sql imdb_job/create_table.sql COPYONLY)
configure_file(imdb_job/q1.sql imdb_job/q1.sql COPYONLY)
configure_file(imdb_job/q2.sql imdb_job/q2.sql COPYONLY)
configure_file(imdb_job/q3.sql imdb_job/q3.sql COPYONLY)
configure_file(imdb_job/q4.sql imdb_job/q4.sql COPYONLY)
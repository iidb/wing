#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "catalog/stat.hpp"
#include "common/stopwatch.hpp"
#include "instance/instance.hpp"
#include "test.hpp"
#include "zipf.hpp"

static const char create_table_sql_path[] = "test/imdb_job/create_table.sql";
static const char test_sql1[] = "test/imdb_job/q1.sql";
static const char test_sql2[] = "test/imdb_job/q2.sql";
static const char test_sql3[] = "test/imdb_job/q3.sql";
static const char test_sql4[] = "test/imdb_job/q4.sql";

static std::vector<std::string> ReadSQLFromFile(const std::string& file_name) {
  std::vector<std::string> ret;
  std::ifstream in(file_name);
  std::string sql = "";
  while (true) {
    std::string line;
    if (!std::getline(in, line)) {
      break;
    }
    uint32_t x = 0;
    uint32_t nx = 0;
    for (; x < line.size(); x++)
      if (line[x] == ';') {
        sql += " " + line.substr(nx, x - nx + 1);
        ret.push_back(sql);
        sql = "";
        nx = x + 1;
      }
    if (nx != x) {
      sql += " " + line.substr(nx, x - nx);
    }
  }
  return ret;
}

static void CreateTables(wing::Instance& db) {
  auto sqls = ReadSQLFromFile(create_table_sql_path);
  for (auto& sql : sqls) {
    ASSERT_TRUE(db.Execute(sql).Valid());
  }
}

static std::pair<size_t, double> GetExecutionTime(
    wing::Instance& db, const std::string& file_name) {
  using namespace wing;
  using namespace wing::wing_testing;
  StopWatch sw;
  auto sqls = ReadSQLFromFile(file_name);
  size_t tuple_counts = 0;
  db.SetEnablePredTrans(true);
  size_t total_output = 0;
  ABORT_ON_TIMEOUT(
      [&]() {
        for (auto sql : sqls) {
          // DB_INFO("{}", db.GetPlan(sql)->ToString());
          auto result = db.Execute(sql);
          ASSERT_TRUE(result.Valid());
          auto tuple = result.Next();
          ASSERT_TRUE(tuple);
          tuple_counts = tuple.ReadInt(0);
          total_output += result.GetTotalOutputSize();
          // if (tuple) {
          //   DB_INFO("{}", tuple.ReadInt(0));
          // }
          DB_INFO("{}", result.GetTotalOutputSize());
        }
      },
      100000);
  db.SetEnablePredTrans(false);
  DB_INFO("{}", total_output);
  return {tuple_counts, sw.GetTimeInSeconds()};
}

static bool CheckData(wing::Instance& db, int movieN, int movieRoleN,
    int movieCompanyN, int castN, int personN, int akaN) {
  auto check_table = [&](auto table_name, int counts) {
    auto rs = db.Execute(fmt::format("select count(*) from {};", table_name));
    if (!rs.Valid())
      return false;
    auto t_counts = rs.Next().ReadInt(0);
    // DB_INFO("{}, {}, {}", table_name, t_counts, counts);
    return t_counts == counts;
  };
  int typeN = 6;
  if (!check_table("role_type", typeN))
    return false;
  if (!check_table("info_type", typeN))
    return false;
  if (!check_table("company_type", typeN))
    return false;
  if (!check_table("company_name", movieCompanyN))
    return false;
  if (!check_table("char_name", movieRoleN))
    return false;
  if (!check_table("name", personN))
    return false;
  if (!check_table("aka_name", akaN))
    return false;
  if (!check_table("title", movieN))
    return false;
  if (!check_table("movie_companies", movieN * 2))
    return false;
  if (!check_table("movie_info", movieN * 2))
    return false;
  if (!check_table("cast_info", castN))
    return false;
  if (!check_table("person_info", personN))
    return false;
  return true;
}

static void GenerateData(wing::Instance& db, int movieN, int movieRoleN,
    int movieCompanyN, int castN, int personN, int akaN) {
  using namespace wing;
  using namespace wing::wing_testing;
  int typeN = 6;
  // Generate role type
  {
    std::string sql = "insert into role_type values";
    for (int i = 0; i < 5; i++)
      sql += fmt::format("(0, '{}'),", (char)('a' + i));
    sql += "(0, 'zzzzend');";
    ASSERT_TRUE(db.Execute(sql).Valid());
  }

  // Generate info type
  {
    std::string sql = "insert into info_type values";
    for (int i = 0; i < 5; i++)
      sql += fmt::format("(0, '{}'),", (char)('a' + i));
    sql += "(0, 'zzzzend');";
    ASSERT_TRUE(db.Execute(sql).Valid());
  }

  // Generate company type
  {
    std::string sql = "insert into company_type values";
    for (int i = 0; i < 5; i++)
      sql += fmt::format("(0, '{}'),", (char)('a' + i));
    sql += "(0, 'zzzzend');";
    ASSERT_TRUE(db.Execute(sql).Valid());
  }

  // Generate company name
  {
    RandomTupleGen tuple_gen(0x202305021731);
    tuple_gen.AddInt(0, 0).AddString(10, 20).AddString(2, 2);
    auto [stmt, data] = tuple_gen.GenerateValuesClause(movieCompanyN);
    ASSERT_TRUE(db.Execute("insert into company_name " + stmt + ";").Valid());
  }

  // Generate char_name (roles)
  {
    RandomTupleGen tuple_gen(0x202305021737);
    tuple_gen.AddInt(0, 0).AddString(10, 20);
    auto [stmt, data] = tuple_gen.GenerateValuesClause(movieRoleN);
    ASSERT_TRUE(db.Execute("insert into char_name " + stmt + ";").Valid());
  }

  // Generate person's name and gender
  {
    RandomTupleGen tuple_gen(0x202305021740);
    tuple_gen.AddInt(0, 0).AddString(10, 20).AddInt(0, 1);
    auto [stmt, data] = tuple_gen.GenerateValuesClause(personN);
    ASSERT_TRUE(db.Execute("insert into name " + stmt + ";").Valid());
  }

  // Generate alternative names (aka_name)
  {
    RandomTupleGen tuple_gen(0x202305021745);
    tuple_gen.AddInt(0, 0).AddInt(1, personN).AddString(10, 20);
    auto [stmt, data] = tuple_gen.GenerateValuesClause(akaN);
    ASSERT_TRUE(db.Execute("insert into aka_name " + stmt + ";").Valid());
  }

  // Generate movie
  {
    RandomTupleGen tuple_gen(0x202305021754);
    tuple_gen.AddInt(0, 0).AddString(10, 20).AddInt(1900, 2023);
    auto [stmt, data] = tuple_gen.GenerateValuesClause(movieN);
    ASSERT_TRUE(db.Execute("insert into title " + stmt + ";").Valid());
  }

  // Generate movie companies
  {
    RandomTupleGen tuple_gen(0x202305021749);
    tuple_gen.AddInt(0, 0)
        .AddInt(1, movieN)
        .AddInt(1, movieCompanyN)
        .AddInt(1, typeN)
        .AddString(10, 20);
    auto [stmt, data] = tuple_gen.GenerateValuesClause(movieN * 2);
    ASSERT_TRUE(
        db.Execute("insert into movie_companies " + stmt + ";").Valid());
  }

  // Generate movie information
  {
    RandomTupleGen tuple_gen(0x202305021749);
    tuple_gen.AddInt(0, 0)
        .AddInt(1, movieN)
        .AddInt(1, typeN)
        .AddString(10, 20)
        .AddString(10, 20);
    auto [stmt, data] = tuple_gen.GenerateValuesClause(movieN * 2);
    ASSERT_TRUE(db.Execute("insert into movie_info " + stmt + ";").Valid());
  }

  // Generate cast information
  {
    RandomTupleGen tuple_gen(0x202305021757);
    tuple_gen.AddInt(0, 0)
        .AddInt(1, personN)
        .AddInt(1, movieN)
        .AddInt(1, movieRoleN)
        .AddString(10, 20)
        .AddInt(1, typeN);
    auto [stmt, data] = tuple_gen.GenerateValuesClause(castN);
    ASSERT_TRUE(db.Execute("insert into cast_info " + stmt + ";").Valid());
  }

  // Generate person info
  {
    RandomTupleGen tuple_gen(0x202305021757);
    tuple_gen.AddInt(0, 0)
        .AddInt(1, personN)
        .AddInt(1, typeN)
        .AddString(10, 20)
        .AddString(10, 20);
    auto [stmt, data] = tuple_gen.GenerateValuesClause(personN);
    ASSERT_TRUE(db.Execute("insert into person_info " + stmt + ";").Valid());
  }
  ASSERT_TRUE(
      CheckData(db, movieN, movieRoleN, movieCompanyN, castN, personN, akaN));
}

static void AnalyzeAllTable(wing::Instance& db) {
  db.Analyze("role_type");
  db.Analyze("info_type");
  db.Analyze("company_type");
  db.Analyze("company_name");
  db.Analyze("char_name");
  db.Analyze("name");
  db.Analyze("aka_name");
  db.Analyze("title");
  db.Analyze("movie_companies");
  db.Analyze("movie_info");
  db.Analyze("cast_info");
  db.Analyze("person_info");
}

static void GenerateDefaultData(wing::Instance& db) {
  // int movieN = 1e6;
  // int movieRoleN = 7e6;
  // int movieCompanyN = 1e4;
  // int castN = 7e6;
  // int personN = 3e5;
  // int akaN = 6e5;

  int movieN = 1e4;
  int movieRoleN = 3e5;
  int movieCompanyN = 1e4;
  int castN = 3e5;
  int personN = 1e4;
  int akaN = 1e4;

  GenerateData(db, movieN, movieRoleN, movieCompanyN, castN, personN, akaN);
}

static bool CheckDefaultData(wing::Instance& db) {
  int movieN = 1e6;
  int movieRoleN = 7e6;
  int movieCompanyN = 1e4;
  int castN = 7e6;
  int personN = 3e5;
  int akaN = 6e5;
  return CheckData(db, movieN, movieRoleN, movieCompanyN, castN, personN, akaN);
}

static void EnsureDB(std::unique_ptr<wing::Instance>& db) {
  bool need_regen = false;
  try {
    auto tmp_db = std::make_unique<wing::Instance>("__imdb", wing_test_options);
    if (!CheckDefaultData(*tmp_db)) {
      need_regen = true;
    }
  } catch (...) {
    need_regen = true;
  }
  if (need_regen) {
    std::filesystem::remove_all("__imdb");
    auto tmp_db = std::make_unique<wing::Instance>("__imdb", wing_test_options);
    CreateTables(*tmp_db);
    DB_INFO("Generating data...");
    GenerateDefaultData(*tmp_db);
    DB_INFO("Complete.");
  } else {
    DB_INFO("Use generated data.");
  }
  db = std::make_unique<wing::Instance>("__imdb", wing_test_options);
}

TEST(Benchmark, JoinOrder10Q1) {
  using namespace wing;
  using namespace wing::wing_testing;
  // std::filesystem::remove_all("__imdb");
  std::filesystem::remove_all("__job_benchmark_result1");
  std::unique_ptr<wing::Instance> db;
  EnsureDB(db);

  AnalyzeAllTable(*db);
  std::ofstream out("__job_benchmark_result1");
  auto [tuple_counts, result] = GetExecutionTime(*db, test_sql1);
  DB_INFO("Use {}s", result);
  out << tuple_counts << " " << result;
}

TEST(Benchmark, JoinOrder10Q2) {
  using namespace wing;
  using namespace wing::wing_testing;
  // std::filesystem::remove_all("__imdb");
  std::filesystem::remove_all("__job_benchmark_result2");
  std::unique_ptr<wing::Instance> db;
  EnsureDB(db);

  AnalyzeAllTable(*db);
  std::ofstream out("__job_benchmark_result2");
  auto [tuple_counts, result] = GetExecutionTime(*db, test_sql2);
  DB_INFO("Use {}s", result);
  out << tuple_counts << " " << result;
}

TEST(Benchmark, JoinOrder10Q3) {
  using namespace wing;
  using namespace wing::wing_testing;
  // std::filesystem::remove_all("__imdb");
  std::filesystem::remove_all("__job_benchmark_result3");
  std::unique_ptr<wing::Instance> db;
  EnsureDB(db);

  AnalyzeAllTable(*db);
  std::ofstream out("__job_benchmark_result3");
  auto [tuple_counts, result] = GetExecutionTime(*db, test_sql3);
  DB_INFO("Use {}s", result);
  out << tuple_counts << " " << result;
}

TEST(Benchmark, JoinOrder10Q4) {
  using namespace wing;
  using namespace wing::wing_testing;
  // std::filesystem::remove_all("__imdb");
  std::filesystem::remove_all("__job_benchmark_result4");
  std::unique_ptr<wing::Instance> db;
  EnsureDB(db);

  AnalyzeAllTable(*db);
  std::ofstream out("__job_benchmark_result4");
  auto [tuple_counts, result] = GetExecutionTime(*db, test_sql4);
  DB_INFO("Use {}s", result);
  out << tuple_counts << " " << result;
}

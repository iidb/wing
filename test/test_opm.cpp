#include <gtest/gtest.h>

#include <filesystem>

#include "catalog/stat.hpp"
#include "common/stopwatch.hpp"
#include "common/threadpool.hpp"
#include "instance/instance.hpp"
#include "test.hpp"
#include "zipf.hpp"

TEST(OptimizerTest, PushdownTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0201");
  auto db = std::make_unique<wing::Instance>("__tmp0201", wing_test_options);
  {
    ResultSet result;
    ABORT_ON_TIMEOUT(
        [&]() {
          // clang-format off
          result = db->Execute("select * from (values(1),(2),(3),(4),(5))A(a),(values(1),(2),(3),(4),(5))B(a),(values(1),(2),(3),(4),(5))C(a),(values(1),(2),(3),(4),(5))D(a),(values(1),(2),(3),(4),(5))E(a),(values(1),(2),(3),(4),(5))F(a),(values(1),(2),(3),(4),(5))G(a),(values(1),(2),(3),(4),(5))H(a),(values(1),(2),(3),(4),(5))I(a),(values(1),(2),(3),(4),(5))J(a),(select * from (select * from (values(1),(2),(3),(4),(5))K(a),(values(1),(2),(3),(4),(5))L(a),(values(1),(2),(3),(4),(5))M(a),(values(1),(2),(3),(4),(5))N(a)),(select * from (values(1),(2),(3),(4),(5))O(a),(values(1),(2),(3),(4),(5))P(a),(values(1),(2),(3),(4),(5))Q(a))),(values(1),(2),(3),(4),(5))R(a),(values(1),(2),(3),(4),(5))S(a),(values(1),(2),(3),(4),(5))T(a),(values(1),(2),(3),(4),(5))U(a),(values(1),(2),(3),(4),(5))V(a),(values(1),(2),(3),(4),(5))W(a),(values(1),(2),(3),(4),(5))X(a),(values(1),(2),(3),(4),(5))Y(a),(values(1),(2),(3),(4),(5))Z(a)where A.a*2<6+A.a and 4<A.a and 1<>A.a and B.a=4 and C.a=2 and D.a=5 and E.a=2 and F.a=4 and G.a+1=3 and H.a=2 and I.a*2=4 and J.a=1 and K.a=K.a*2-4 and 1=L.a and M.a=5 and N.a=4 and O.a=3 and P.a>0 and P.a<2 and Q.a=1 and R.a=1 and S.a>2 and S.a<4 and T.a>4 and U.a=3 and V.a=1 and W.a=3 and X.a=5+X.a-X.a and Y.a=1 and Z.a=5 and 1=1;");
          // clang-format on
        },
        1000);
    EXPECT_TRUE(result.Valid());
    AnsVec answer;
    answer.push_back(MkVec(Value::CreateInt(5), Value::CreateInt(4),
        Value::CreateInt(2), Value::CreateInt(5), Value::CreateInt(2),
        Value::CreateInt(4), Value::CreateInt(2), Value::CreateInt(2),
        Value::CreateInt(2), Value::CreateInt(1), Value::CreateInt(4),
        Value::CreateInt(1), Value::CreateInt(5), Value::CreateInt(4),
        Value::CreateInt(3), Value::CreateInt(1), Value::CreateInt(1),
        Value::CreateInt(1), Value::CreateInt(3), Value::CreateInt(5),
        Value::CreateInt(3), Value::CreateInt(1), Value::CreateInt(3),
        Value::CreateInt(5), Value::CreateInt(1), Value::CreateInt(5)));
    CHECK_ALL_SORTED_ANS(answer, result, 26);
  }

  {
    EXPECT_TRUE(
        db->Execute("create table A(a int64 primary key, b int64);").Valid());
    EXPECT_TRUE(
        db->Execute("create table B(a int64 primary key, b int64);").Valid());
    EXPECT_TRUE(
        db->Execute("create table C(a int64 primary key, b int64);").Valid());
    RandomTupleGen tuple_gen(202303041622ull);
    tuple_gen.AddInt().AddInt(0, 100);
    int NUM = 1e4;
    auto [stmt_a, data_a] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into A " + stmt_a + ";").Valid());
    auto [stmt_b, data_b] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into B " + stmt_b + ";").Valid());
    auto [stmt_c, data_c] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into C " + stmt_c + ";").Valid());
    ResultSet result;
    ABORT_ON_TIMEOUT(
        [&]() {
          // clang-format off
          result = db->Execute("select A.a, B.a, C.a from A, B, C where ((A.a ^ B.a ^ C.a) & 6) = 6 and A.b = 0 and B.b = 0 and C.b = 0 order by A.a asc, B.a asc, C.a asc;");
          // clang-format on
        },
        1000);
    std::vector<int64_t> A, B, C;
    for (int i = 0; i < NUM; i++)
      if (data_a.Get(i, 1).ReadInt() == 0)
        A.push_back(data_a.Get(i, 0).ReadInt());
    for (int i = 0; i < NUM; i++)
      if (data_b.Get(i, 1).ReadInt() == 0)
        B.push_back(data_b.Get(i, 0).ReadInt());
    for (int i = 0; i < NUM; i++)
      if (data_c.Get(i, 1).ReadInt() == 0)
        C.push_back(data_c.Get(i, 0).ReadInt());
    std::sort(A.begin(), A.end());
    std::sort(B.begin(), B.end());
    std::sort(C.begin(), C.end());
    AnsVec answer;
    for (auto a : A)
      for (auto b : B)
        for (auto c : C)
          if (((a ^ b ^ c) & 6) == 6) {
            answer.push_back(MkVec(
                Value::CreateInt(a), Value::CreateInt(b), Value::CreateInt(c)));
          }
    CHECK_ALL_SORTED_ANS(answer, result, 3);
  }

  db = nullptr;
  std::filesystem::remove_all("__tmp0201");
}

// TEST(OptimizerTest, ProjectFoldTest) {
//   using namespace wing;
//   using namespace wing::wing_testing;
//   std::filesystem::remove_all("__tmp0202");
//   auto db = std::make_unique<wing::Instance>("__tmp0202",
//   wing_test_options);
//   {
//     EXPECT_TRUE(
//         db->Execute("create table A(a int64 primary key, b int64, c
//         float64);")
//             .Valid());
//     RandomTupleGen<int64_t, int64_t, double> tuple_gen(
//         202303051133ull, INT64_MIN, INT64_MAX, 0, 100, 0.0, 1.0);
//     int NUM = 5e3;
//     auto [stmt_a, data_a] = tuple_gen.GenerateValuesClause(NUM);
//     EXPECT_TRUE(db->Execute("insert into A " + stmt_a + ";").Valid());
//     StopWatch sw;
//     ABORT_ON_TIMEOUT(
//         [&]() {
//           // clang-format off
//           db->Execute("select b0, b3, sum(b2), sum(b1), sum(b4), avg(b5) from
//           (select * from (select a0 * a0 +a3 as b0, a1 * a1 as b1, a2 * a2 as
//           b2, a3 * a3 + a0 as b3, a4 * a1 as b4, a5 * a2-1 as b5 from (select
//           LA.a as a0, LA.b as a1, LA.c as a2, RA.a as a3, RA.b as a4, RA.c as
//           a5 from (select LA.a, RA.a, LA.b, RA.b, LA.c, RA.c from(select *
//           from(select * from(select * from(select * from (select * from
//           (select * from A as LA, A as RA)))))))))) group by b0 & 7, b3 & 7
//           order by b0 & 7 asc, b3 & 7 asc;");
//           // clang-format on
//         },
//         4000);
//     DB_INFO("Use: {} s", sw.GetTimeInSeconds());
//   }
//   db = nullptr;
//   std::filesystem::remove_all("__tmp0202");
// }

TEST(StatsTest, HyperLLTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  HyperLL hll(1024);
  for (int i = 0; i < 10000000; i++) {
    hll.Add({reinterpret_cast<char*>(&i), 4});
  }
  for (int i = 0; i < 10000000; i++) {
    hll.Add({reinterpret_cast<char*>(&i), 4});
  }
  auto ret = hll.GetDistinctCounts();
  EXPECT_TRUE(fabs(ret - 1e7) / 1e7 < 0.05);
}

TEST(StatsTest, CountMinSketchTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::mt19937_64 gen(0x202303061813);
  zipf_distribution<> zipf(100000);
  // ans <= real + e / 0.0013 * N
  // where N is the total count, 1e7.
  int N = 2e7;
  CountMinSketch sk(2000, 8);
  std::vector<int> seq(100000);
  std::vector<int> st(100000);
  for (int i = 0; i < 100000; i++)
    seq[i] = i;
  std::shuffle(seq.begin(), seq.end(), gen);
  for (int i = 0; i < N; i++) {
    int j = seq[zipf(gen) - 1];
    st[j]++;
    sk.AddCount({reinterpret_cast<char*>(&j), 4});
  }
  for (int i = 0; i < 100000; i++)
    if (st[i] > 1000) {
      double freq_c = sk.GetFreqCount({reinterpret_cast<char*>(&i), 4});
      EXPECT_TRUE(fabs(freq_c - st[i]) / (double)N < 0.002);
    }
}

TEST(OptimizerTest, JoinCommuteTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0203");
  auto db = std::make_unique<wing::Instance>("__tmp0203", wing_test_options);
  db->SetEnableCostBased(true);
  {
    EXPECT_TRUE(db->Execute("create table B(a int64 auto_increment primary "
                            "key, data varchar(2));")
                    .Valid());
    EXPECT_TRUE(db->Execute("create table A(a int64 foreign key references "
                            "B(a), b int64, c float64);")
                    .Valid());
    int NUMA = 1e6, NUMB = 1e3;
    RandomTupleGen tuple_gen(202303051627ull);
    tuple_gen.AddInt(1, NUMB).AddInt(0, 10000).AddFloat(0, 1);
    DB_INFO("Generating data...");
    StopWatch _sw;
    RandomTupleGen tuple_gen2(20230305162702ull);
    tuple_gen2.AddInt(0, 0).AddString(1, 2);
    auto stmt_b = tuple_gen2.GenerateValuesClauseStmt(NUMB);
    EXPECT_TRUE(db->Execute("insert into B " + stmt_b + ";").Valid());
    stmt_b.clear();
    DB_INFO("Insert into B uses {} s.", _sw.GetTimeInSeconds());
    _sw.Reset();

    auto stmt_a = tuple_gen.GenerateValuesClauseStmt(NUMA);
    EXPECT_TRUE(db->Execute("insert into A " + stmt_a + ";").Valid());
    DB_INFO("Insert into A uses {} s.", _sw.GetTimeInSeconds());
    stmt_a.clear();

    DB_INFO("Data generation completed.");
    db->Analyze("A");
    db->Analyze("B");
    DB_INFO("Database analyzed.");
    // StopWatch sw;
    // You should swap A, B. because B is much smaller than A.

    // ABORT_ON_TIMEOUT(
    //     [&]() {
    //       // clang-format off
    //       db->Execute("select * from A, B where A.a = B.a;");
    //       // clang-format on
    //     },
    //     3000);
    {
      auto pplan = db->GetPlan("select * from A, B where A.a = B.a;");
      ASSERT_TRUE(pplan->type_ == PlanType::Project);
      ASSERT_TRUE(pplan->ch_->type_ == PlanType::HashJoin);
      auto plan = std::move(pplan->ch_);
      ASSERT_TRUE(plan->ch_ && (plan->ch_->type_ == PlanType::SeqScan ||
                                   plan->ch_->type_ == PlanType::RangeScan));
      ASSERT_TRUE(plan->ch2_ && (plan->ch2_->type_ == PlanType::SeqScan ||
                                    plan->ch2_->type_ == PlanType::RangeScan));
      if (plan->ch_->type_ == PlanType::SeqScan) {
        ASSERT_TRUE(
            static_cast<SeqScanPlanNode*>(plan->ch_.get())->table_name_ == "B");
      } else {
        ASSERT_TRUE(
            static_cast<RangeScanPlanNode*>(plan->ch_.get())->table_name_ ==
            "B");
      }
      if (plan->ch2_->type_ == PlanType::SeqScan) {
        ASSERT_TRUE(
            static_cast<SeqScanPlanNode*>(plan->ch2_.get())->table_name_ ==
            "A");
      } else {
        ASSERT_TRUE(
            static_cast<RangeScanPlanNode*>(plan->ch2_.get())->table_name_ ==
            "A");
      }
    }

    // DB_INFO("Use: {} s", sw.GetTimeInSeconds());
    // sw.Reset();

    // You should swap dupA and A. because b is uniformly distributed in [0,
    // 100]. So dupA's size is 1e6 / 10000.

    // ABORT_ON_TIMEOUT(
    //     [&]() {
    //       // clang-format off
    //       db->Execute("select * from A, A as dupA where A.a = dupA.a and
    //       dupA.b = 5;");
    //       // clang-format on
    //     },
    //     3000);
    // DB_INFO("Use: {} s", sw.GetTimeInSeconds());

    {
      auto pplan = db->GetPlan(
          "select * from A, A as dupA where A.a = dupA.a and dupA.b = 5;");
      ASSERT_TRUE(pplan->type_ == PlanType::Project);
      ASSERT_TRUE(pplan->ch_->type_ == PlanType::HashJoin);
      auto plan = std::move(pplan->ch_);
      ASSERT_TRUE(plan->ch_ && (plan->ch_->type_ == PlanType::SeqScan ||
                                   plan->ch_->type_ == PlanType::RangeScan));
      ASSERT_TRUE(plan->ch2_ && (plan->ch2_->type_ == PlanType::SeqScan ||
                                    plan->ch2_->type_ == PlanType::RangeScan));
      if (plan->ch_->type_ == PlanType::SeqScan) {
        ASSERT_TRUE(static_cast<SeqScanPlanNode*>(plan->ch_.get())
                        ->predicate_.GetVec()
                        .size() != 0);
      } else {
        ASSERT_TRUE(static_cast<RangeScanPlanNode*>(plan->ch_.get())
                        ->predicate_.GetVec()
                        .size() != 0);
      }
      if (plan->ch2_->type_ == PlanType::SeqScan) {
        ASSERT_TRUE(static_cast<SeqScanPlanNode*>(plan->ch2_.get())
                        ->predicate_.GetVec()
                        .size() == 0);
      } else {
        ASSERT_TRUE(static_cast<RangeScanPlanNode*>(plan->ch2_.get())
                        ->predicate_.GetVec()
                        .size() == 0);
      }
    }

    {
      auto pplan = db->GetPlan(
          "select * from A as dupA, A where A.a = dupA.a and dupA.b = 5;");
      ASSERT_TRUE(pplan->type_ == PlanType::Project);
      ASSERT_TRUE(pplan->ch_->type_ == PlanType::HashJoin);
      auto plan = std::move(pplan->ch_);
      ASSERT_TRUE(plan->ch_ && (plan->ch_->type_ == PlanType::SeqScan ||
                                   plan->ch_->type_ == PlanType::RangeScan));
      ASSERT_TRUE(plan->ch2_ && (plan->ch2_->type_ == PlanType::SeqScan ||
                                    plan->ch2_->type_ == PlanType::RangeScan));
      if (plan->ch_->type_ == PlanType::SeqScan) {
        ASSERT_TRUE(static_cast<SeqScanPlanNode*>(plan->ch_.get())
                        ->predicate_.GetVec()
                        .size() != 0);
      } else {
        ASSERT_TRUE(static_cast<RangeScanPlanNode*>(plan->ch_.get())
                        ->predicate_.GetVec()
                        .size() != 0);
      }
      if (plan->ch2_->type_ == PlanType::SeqScan) {
        ASSERT_TRUE(static_cast<SeqScanPlanNode*>(plan->ch2_.get())
                        ->predicate_.GetVec()
                        .size() == 0);
      } else {
        ASSERT_TRUE(static_cast<RangeScanPlanNode*>(plan->ch2_.get())
                        ->predicate_.GetVec()
                        .size() == 0);
      }
    }
  }

  db = nullptr;
  std::filesystem::remove_all("__tmp0203");
}

TEST(OptimizerTest, JoinAssociate4Test) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0204");
  auto db = std::make_unique<wing::Instance>("__tmp0204", wing_test_options);
  db->SetEnableCostBased(true);
  // There is a cafe called Cats'eye which has some cat employees.
  // The cat dba only stores the id of the referenced information in table Cats.
  // Now she wants to print these information for each cat.
  {
    EXPECT_TRUE(db->Execute("create table Cats(emp_id int64, cat_id int64, "
                            "username_id int64);")
                    .Valid());
    EXPECT_TRUE(db->Execute("create table Employee(id int64 auto_increment "
                            "primary key, name varchar(20));")
                    .Valid());
    EXPECT_TRUE(db->Execute("create table Username(id int64 auto_increment "
                            "primary key, name varchar(20));")
                    .Valid());
    EXPECT_TRUE(db->Execute("create table Catname(id int64 auto_increment "
                            "primary key, name varchar(20));")
                    .Valid());
    int CATNUM = 1e3;
    int NUM = 1e5;
    RandomTupleGen tuple_gen(0x202303091108);
    tuple_gen.AddInt(0, 0).AddString(20, 20);
    auto [stmt_e, data_e] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Employee " + stmt_e + ";").Valid());
    auto [stmt_u, data_u] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Username " + stmt_u + ";").Valid());
    auto [stmt_c, data_c] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Catname " + stmt_c + ";").Valid());
    RandomTupleGen tuple_gen2(0x202303091110);
    tuple_gen2.AddInt(1, NUM).AddInt(1, NUM).AddInt(1, NUM);
    auto [stmt_cats, data_cats] = tuple_gen2.GenerateValuesClause(CATNUM);
    EXPECT_TRUE(db->Execute("insert into Cats " + stmt_cats + ";").Valid());

    db->Analyze("Employee");
    db->Analyze("Username");
    db->Analyze("Catname");
    db->Analyze("Cats");

    StopWatch sw;
    ABORT_ON_TIMEOUT(
        [&]() {
          // clang-format off
          db->Execute("select * from Employee, Username, Catname, Cats where Cats.emp_id = Employee.id and Cats.username_id = Username.id and Cats.cat_id = Catname.id;");
          // clang-format on
        },
        5000);
    DB_INFO("Use: {} s", sw.GetTimeInSeconds());
  }
  db = nullptr;
  std::filesystem::remove_all("__tmp0204");
}

TEST(OptimizerTest, JoinAssociate5Test) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0205");
  auto db = std::make_unique<wing::Instance>("__tmp0205", wing_test_options);
  db->SetEnableCostBased(true);
  // There is a cafe called Cats'eye which has some cat employees.
  // The cat dba only stores the id of the referenced information in table Cats.
  // Now she wants to print these information for each cat.
  // Different from JoinAssociate5Test, there are 5 tables.
  {
    EXPECT_TRUE(db->Execute("create table Cats(emp_id int64, cat_id int64, "
                            "username_id int64, maid_id int64);")
                    .Valid());
    EXPECT_TRUE(db->Execute("create table Employee(id int64 auto_increment "
                            "primary key, name varchar(20));")
                    .Valid());
    EXPECT_TRUE(db->Execute("create table Username(id int64 auto_increment "
                            "primary key, name varchar(20));")
                    .Valid());
    EXPECT_TRUE(db->Execute("create table Catname(id int64 auto_increment "
                            "primary key, name varchar(20));")
                    .Valid());
    EXPECT_TRUE(db->Execute("create table Maidname(id int64 auto_increment "
                            "primary key, name varchar(20));")
                    .Valid());
    int CATNUM = 1e5;
    int NUM = 1e5;
    RandomTupleGen tuple_gen(0x202303091108);
    tuple_gen.AddInt(0, 0).AddString(20, 20);
    auto [stmt_e, data_e] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Employee " + stmt_e + ";").Valid());
    auto [stmt_u, data_u] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Username " + stmt_u + ";").Valid());
    auto [stmt_c, data_c] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Catname " + stmt_c + ";").Valid());
    auto [stmt_m, data_m] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Maidname " + stmt_m + ";").Valid());
    RandomTupleGen tuple_gen2(0x202303091110);
    tuple_gen2.AddInt(1, NUM).AddInt(1, NUM).AddInt(1, NUM).AddInt(1, NUM);
    auto [stmt_cats, data_cats] = tuple_gen2.GenerateValuesClause(CATNUM);
    EXPECT_TRUE(db->Execute("insert into Cats " + stmt_cats + ";").Valid());

    db->Analyze("Employee");
    db->Analyze("Username");
    db->Analyze("Catname");
    db->Analyze("Cats");
    db->Analyze("Maidname");

    int test_round = 10;

    StopWatch sw;
    ABORT_ON_TIMEOUT(
        [&]() {
          std::mt19937_64 rgen(0x202304172253);
          std::vector<std::string> tables = {
              "Employee", " Username", "Catname", " Maidname", "Cats"};
          // clang-format off
          std::string predicate = "Cats.emp_id = Employee.id and Cats.username_id = Username.id and Cats.cat_id = Catname.id and Cats.maid_id = Maidname.id";
          // clang-format on
          for (int i = 0; i < test_round; i++) {
            std::shuffle(tables.begin(), tables.end(), rgen);
            db->Execute(fmt::format(
                "select * from {}, {}, {}, {}, {} where {};", tables[0],
                tables[1], tables[2], tables[3], tables[4], predicate));
          }
        },
        6000);
    DB_INFO("Use: {} s", sw.GetTimeInSeconds());
    {
      ResultSet result;

      StopWatch sw;
      ABORT_ON_TIMEOUT(
          [&]() {
            result = db->Execute(
                "select count(*) from Cats as B, Cats as C, Cats as D, Cats as "
                "E,  Cats as A where "
                "A.cat_id = B.cat_id and A.emp_id = B.emp_id and A.username_id "
                "= B.username_id "
                "and B.cat_id = C.cat_id and B.maid_id = C.maid_id and "
                "B.username_id = C.username_id "
                "and B.maid_id = E.maid_id and B.emp_id = E.emp_id and "
                "B.username_id = E.username_id "
                "and E.cat_id = D.cat_id and D.emp_id = B.emp_id and "
                "D.username_id = B.username_id; ");
          },
          2000);
      DB_INFO("Use: {} s", sw.GetTimeInSeconds());
      EXPECT_TRUE(result.Valid());
      auto tuple = result.Next();
      EXPECT_TRUE(tuple);
      EXPECT_EQ(tuple.ReadInt(0), CATNUM);
    }
  }
  db = nullptr;
  std::filesystem::remove_all("__tmp0205");
}

template <typename PKType, typename T,
    T (wing::wing_testing::Value::*ReadPKMethod)() const,
    typename GenRandomKeyFunc, typename RTGenType, typename DBType,
    typename GenValueClauseFunc>
void TestPKRangeScan(int NUM, int round, int short_range_round,
    int short_range_len, int long_range_round, RTGenType&& tuple_gen,
    DBType* db, GenRandomKeyFunc&& rgen_func, GenValueClauseFunc&& vc_func) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::vector<std::pair<std::string, ValueVector>> stmt_data;
  for (int i = 0; i < round; i++)
    stmt_data.push_back(tuple_gen.GenerateValuesClause(NUM / round));
  std::map<PKType, double> mp;
  // Used to generate op. Not data.
  // For example, op_rgen() % 2 ? Insert(key, value) : Read(key).
  std::mt19937_64 op_rgen(0x202304172350);

  StopWatch sw;
  ABORT_ON_TIMEOUT(
      [&]() {
        // Test point lookup.
        for (int i = 0; i < round; i++) {
          // Insert data.
          EXPECT_TRUE(
              db->Execute("insert into A " + stmt_data[i].first + ";").Valid());
          // Insert data into mp.
          for (int j = 0; j < NUM / round; j++) {
            auto key = PKType((stmt_data[i].second.Get(j, 0).*ReadPKMethod)());
            auto value = stmt_data[i].second.Get(j, 1).ReadFloat();
            if (!mp.count(key))
              mp[key] = value;
          }
          PKType random_key = rgen_func();
          if (op_rgen() % 2) {
            auto it = mp.lower_bound(random_key);
            if (it != mp.end()) {
              random_key = it->first;
            }
          }
          auto result = db->Execute(fmt::format(
              "select * from A where a = {};", vc_func(random_key)));
          EXPECT_TRUE(result.Valid());
          auto tuple = result.Next();
          if (mp.count(random_key)) {
            EXPECT_TRUE(tuple);
            EXPECT_EQ(tuple.ReadFloat(1), mp[random_key]);
            EXPECT_FALSE(result.Next());
          } else {
            EXPECT_FALSE(tuple);
          }
        }
      },
      10000);
  DB_INFO("Use: {} s", sw.GetTimeInSeconds());
  sw.Reset();
  ABORT_ON_TIMEOUT(
      [&]() {
        // Test short range scan. short range length is short_range_len (100).
        for (int i = 0; i < short_range_round; i++) {
          PKType L = rgen_func();
          PKType R = L;
          auto it = mp.lower_bound(L);
          for (int j = 0; j < short_range_len && it != mp.end(); j++) {
            it++;
          }
          if (it != mp.end())
            R = it->first;
          auto result = db->Execute(
              fmt::format("select * from A where a >= {} and a < {};",
                  vc_func(L), vc_func(R)));
          it = mp.lower_bound(L);
          while (it != mp.end() && it->first < R) {
            auto tuple = result.Next();
            EXPECT_TRUE(tuple);
            EXPECT_EQ(tuple.ReadFloat(1), it->second);
            it++;
          }
          EXPECT_FALSE(result.Next());
        }
        // Test long range scan. The range is either (a, inf) or (-inf, a) or
        // [a, inf) or (-inf, a].
        std::string ops[] = {">", "<", ">=", "<="};
        for (int j = 0; j < 4; ++j) {
          for (int i = 0; i < long_range_round; i++) {
            PKType L = rgen_func();
            auto result = db->Execute(fmt::format(
                "select * from A where a {} {};", ops[j], vc_func(L)));
            typename std::map<PKType, double>::iterator it;
            if (ops[j] == ">") {
              it = mp.upper_bound(L);
              while (it != mp.end()) {
                auto tuple = result.Next();
                EXPECT_TRUE(tuple);
                EXPECT_EQ(tuple.ReadFloat(1), it->second);
                it++;
              }
              EXPECT_FALSE(result.Next());
            } else if (ops[j] == ">=") {
              it = mp.lower_bound(L);
              while (it != mp.end()) {
                auto tuple = result.Next();
                EXPECT_TRUE(tuple);
                EXPECT_EQ(tuple.ReadFloat(1), it->second);
                it++;
              }
              EXPECT_FALSE(result.Next());
            } else if (ops[j] == "<") {
              it = mp.begin();
              while (it != mp.end() && it->first < L) {
                auto tuple = result.Next();
                EXPECT_TRUE(tuple);
                EXPECT_EQ(tuple.ReadFloat(1), it->second);
                it++;
              }
              EXPECT_FALSE(result.Next());
            } else {
              it = mp.begin();
              while (it != mp.end() && it->first <= L) {
                auto tuple = result.Next();
                EXPECT_TRUE(tuple);
                EXPECT_EQ(tuple.ReadFloat(1), it->second);
                it++;
              }
              EXPECT_FALSE(result.Next());
            }
          }
        }
      },
      10000);
  DB_INFO("Use: {} s", sw.GetTimeInSeconds());
  sw.Reset();

  // Test empty range.
  ABORT_ON_TIMEOUT(
      [&]() {
        for (int i = 0; i < 100; i++) {
          PKType x = rgen_func();
          while (mp.count(x))
            x = rgen_func();
          auto result = db->Execute(
              fmt::format("select * from A where a > {} and a < {}; ",
                  vc_func(x), vc_func(x)));
          EXPECT_TRUE(result.Valid());
          EXPECT_FALSE(result.Next());
        }
      },
      3000);
  DB_INFO("Use: {} s", sw.GetTimeInSeconds());
}

TEST(OptimizerTest, IntegerRangeScanTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0205");
  auto db = std::make_unique<wing::Instance>("__tmp0205", wing_test_options);
  // Insert key-value data
  // Read the corresponding value of a random key while inserting.
  {
    EXPECT_TRUE(
        db->Execute("create table A(a int64 primary key, b float64);").Valid());
    int NUM = 1e6, round = 2e5, short_range_round = 5e4, short_range_len = 100;
    int long_range_round = 10;
    std::mt19937_64 rgen(0x202304041223);
    RandomTupleGen tuple_gen(0x202304041203ll);
    tuple_gen.AddInt().AddFloat(0, 1);
    TestPKRangeScan<int64_t, int64_t, &Value::ReadInt>(
        NUM, round, short_range_round, short_range_len, long_range_round,
        tuple_gen, db.get(), [&rgen]() { return rgen(); },
        [](int64_t x) { return x; });
  }
  db = nullptr;
  std::filesystem::remove_all("__tmp0205");
}

TEST(OptimizerTest, StringRangeScanTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0206");
  auto db = std::make_unique<wing::Instance>("__tmp0206", wing_test_options);
  {
    EXPECT_TRUE(
        db->Execute("create table A(a varchar(20) primary key, b float64);")
            .Valid());
    int NUM = 2e5, round = 2e4, short_range_round = 1e4, short_range_len = 100;
    int long_range_round = 10;
    std::mt19937_64 rgen(0x20230418033);
    RandomTupleGen tuple_gen(0x202304041203ll);
    tuple_gen.AddString(10, 20).AddFloat();
    TestPKRangeScan<std::string, std::string_view, &Value::ReadString>(
        NUM, round, short_range_round, short_range_len, long_range_round,
        tuple_gen, db.get(),
        [&rgen]() -> std::string {
          std::string ret(rgen() % 20 + 10, 0);
          char char_set[] =
              "abcdefghijklmnopqrstuvwxyzQWERTYUIOPLKJHGFDSANMZXCVB?:{["
              "1234567890-="
              "/.,!@#$%^&*()]}";
          for (auto& c : ret)
            c = char_set[rgen() % sizeof(char_set)];
          return ret;
        },
        [](std::string x) { return "'" + x + "'"; });
  }
  db = nullptr;
  std::filesystem::remove_all("__tmp0206");
}

TEST(OptimizerTest, FloatRangeScanTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0207");
  auto db = std::make_unique<wing::Instance>("__tmp0207", wing_test_options);
  // Insert key-value data
  // Read the corresponding value of a random key while inserting.
  {
    EXPECT_TRUE(db->Execute("create table A(a float64 primary key, b float64);")
                    .Valid());
    int NUM = 5e5, round = 1e5, short_range_round = 1e4, short_range_len = 100;
    int long_range_round = 10;
    std::mt19937_64 rgen(0x202304041223);
    RandomTupleGen tuple_gen(0x202304041203ll);
    tuple_gen.AddFloat(0, 1e4).AddFloat(0, 1);
    TestPKRangeScan<double, double, &Value::ReadFloat>(
        NUM, round, short_range_round, short_range_len, long_range_round,
        tuple_gen, db.get(),
        [&rgen]() -> double {
          std::uniform_real_distribution<> dis(0.0, 1e4);
          return std::floor(dis(rgen) * 1e4) / 1e4;
        },
        [](double x) -> std::string { return fmt::format("{:.10f}", x); });
  }
  db = nullptr;
  std::filesystem::remove_all("__tmp0207");
}

class PredElement {
 public:
  std::string pred_str;
  std::vector<std::string> tables;

  PredElement(std::string _pred_str, std::vector<std::string> _tables)
    : pred_str(_pred_str), tables(_tables) {}
};

std::vector<std::pair<std::vector<std::string>, double>> CalcTrueCard(
    std::vector<std::string> tables, std::vector<PredElement> preds,
    wing::Instance& db) {
  DB_ASSERT(tables.size() < 20);
  std::vector<std::pair<std::vector<std::string>, double>> true_card(
      (1 << tables.size()));
  std::vector<std::tuple<size_t, size_t, std::string>> not_calc;
  wing::ThreadPool threads;
  true_card[0] = {std::vector<std::string>(), 0};
  db.SetEnableCostBased(true);
  for (uint32_t S = 1; S < true_card.size(); S++) {
    std::vector<std::string> table_subset;
    std::string table_str;
    std::string pred_str;
    for (uint32_t j = 0; j < tables.size(); ++j) {
      if ((S >> j) & 1) {
        table_subset.push_back(tables[j]);
        if (table_str.empty()) {
          table_str = tables[j];
        } else {
          table_str += ", ";
          table_str += tables[j];
        }
      }
    }
    true_card[S].first = table_subset;
    // Check if the query graph is connected.
    // If not, then we can find a connected component that is a subset of the
    // current table set `i`.
    std::vector<std::pair<int, uint32_t>> union_set(tables.size());
    // top of the union set,
    // use it to get the result.
    uint32_t union_set_tp = 0;
    for (uint32_t l = 0; l < tables.size(); l++)
      if ((S >> l) & 1) {
        union_set_tp = l;
        union_set[l] = {l, 1 << l};
      }
    auto find = [&](int t) {
      while (t != union_set[t].first) {
        int ft = union_set[t].first;
        union_set[ft].second |= union_set[t].second;
        union_set[t].first = union_set[union_set[ft].first].first;
        t = ft;
      }
      return t;
    };
    for (uint32_t j = 0; j < preds.size(); ++j) {
      bool flag2 = true;
      // enumerate all tables in the predicate
      size_t table_set = 0;
      for (uint32_t k = 0; k < preds[j].tables.size(); k++) {
        bool flag = false;
        // get the index of the table.
        for (uint32_t l = 0; l < tables.size(); l++) {
          if ((S >> l) & 1) {
            if (tables[l] == preds[j].tables[k]) {
              flag = true;
              table_set |= 1 << l;
              break;
            }
          }
        }
        if (!flag) {
          flag2 = false;
          break;
        }
      }
      if (flag2) {
        if (__builtin_popcountll(table_set) > 1) {
          int x = -1;
          for (uint32_t l = 0; l < tables.size(); l++) {
            if ((table_set >> l) & 1) {
              if (x != -1) {
                int fx = find(x);
                int fl = find(l);
                union_set[fl].first = union_set[fx].first;
                union_set[fx].second |= union_set[fl].second;
              } else {
                x = l;
              }
            }
          }
        }

        if (pred_str.empty()) {
          pred_str = "(";
          pred_str += preds[j].pred_str;
          pred_str += ")";
        } else {
          pred_str += " and (";
          pred_str += preds[j].pred_str;
          pred_str += ")";
        }
      }
    }

    std::string sql = fmt::format("select 1 from {} {} {};", table_str,
        pred_str.empty() ? "" : "where", pred_str);
    if (uint32_t subset = union_set[find(union_set_tp)].second; subset != S) {
      not_calc.emplace_back(S, subset, sql);
      continue;
    }
    DB_INFO("{}", sql);
    threads.Push([&db, &true_card, S, sql]() {
      auto ret = db.Execute(sql);
      DB_ASSERT(ret.Valid());
      true_card[S].second = 0;
      while (ret.Next()) {
        true_card[S].second += 1;
      }
      DB_INFO("{}: {}", sql, true_card[S].second);
    });
  }
  threads.WaitForAllTasks();
  for (auto& [S, subset, sql] : not_calc) {
    true_card[S].second =
        true_card[S ^ subset].second * true_card[subset].second;
    std::string subset_table_str;
    for (uint32_t l = 0; l < tables.size(); l++)
      if ((subset >> l) & 1) {
        subset_table_str += tables[l] + ", ";
      }
    DB_INFO(
        "subset {{ {} }}, {}: {}", subset_table_str, sql, true_card[S].second);
  }
  return true_card;
}

void PrintTrueCard(
    const std::vector<std::pair<std::vector<std::string>, double>>& true_cards,
    const std::string& file_name) {
  if (file_name == "")
    return;
  std::ofstream out(file_name, std::ios::binary);
  out << true_cards.size() << std::endl;
  for (auto& [tables, size] : true_cards) {
    out << tables.size() << " ";
    for (auto& a : tables) {
      out << a << " ";
    }
    out << ": " << std::setprecision(20) << size << "\n";
  }
  out << std::endl;
}

std::optional<std::vector<std::pair<std::vector<std::string>, double>>>
ReadTrueCard(const std::string& file_name) {
  if (file_name == "")
    return {};
  std::ifstream in(file_name, std::ios::binary);
  if (!in || in.eof()) {
    return {};
  }
  size_t num = 0;
  if (!(in >> num) || !num) {
    return {};
  }
  std::vector<std::pair<std::vector<std::string>, double>> data;
  for (size_t i = 0; i < num; i++) {
    std::vector<std::string> tables;
    double size;
    size_t table_num = 0;
    if (!(in >> table_num)) {
      return {};
    }
    for (size_t j = 0; j < table_num; j++) {
      std::string name;
      if (!(in >> name)) {
        return {};
      }
      tables.push_back(name);
    }
    std::string colon, sql;
    in >> colon;
    if (colon != ":" || !(in >> size)) {
      return {};
    }
    data.emplace_back(tables, size);
  }
  return data;
}

TEST(EasyOptimizerTest, Join3Tables) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0208");
  auto db = std::make_unique<wing::Instance>("__tmp0208", wing_test_options);
  EXPECT_TRUE(db->Execute("create table t1(id int64, idt3 int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t2(id int64, idt1 int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t3(id int64, idt2 int64);").Valid());
  int N = 1000;
  RandomTupleGen tuple_gen(0x202405281635);
  tuple_gen.AddInt(1, 100).AddInt(1, 100);
  auto [stmt_t1, data_t1] = tuple_gen.GenerateValuesClause(N);
  ASSERT_TRUE(db->Execute("insert into t1 " + stmt_t1 + ";").Valid());
  auto [stmt_t2, data_t2] = tuple_gen.GenerateValuesClause(N);
  ASSERT_TRUE(db->Execute("insert into t2 " + stmt_t2 + ";").Valid());
  auto [stmt_t3, data_t3] = tuple_gen.GenerateValuesClause(N);
  ASSERT_TRUE(db->Execute("insert into t3 " + stmt_t3 + ";").Valid());
  auto true_card = CalcTrueCard({"t1", "t2", "t3"},
      {PredElement("t1.id = t2.idt1", {"t1", "t2"}),
          PredElement("t2.id = t3.idt2", {"t2", "t3"}),
          PredElement("t3.id = t1.idt3", {"t1", "t3"})},
      *db);
  db->SetDebugPrintPlan(true);
  db->SetEnableCostBased(true);
  db->SetTrueCardinalityHints(true_card);
  auto ret = db->Execute(
      "select 1 from t1, t2, t3 where t1.id = t2.idt1 and t2.id = t3.idt2 and "
      "t3.id = t1.idt3;");
  ASSERT_TRUE(ret.Valid());
  DB_INFO("{}, {}", ret.GetTotalOutputSize(), ret.GetPlan()->cost_);
  // standard answer: join(join (t1, t3), t2)
  ASSERT_EQ(ret.GetTotalOutputSize(), 14784);  // 1000+1000+1000+9898+943+943
  ASSERT_EQ(ret.GetPlan()->cost_,
      142.821);  // 3000*0.001+9898*0.001+(1000+1000)*0.01+(1000+9898)*0.01+943*0.001
  ASSERT_EQ(true_card.back().second, ret.GetSize());
  db = nullptr;
  std::filesystem::remove_all("__tmp0208");
}

TEST(EasyOptimizerTest, Join5TablesCrossProduct) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0209");
  auto db = std::make_unique<wing::Instance>("__tmp0209", wing_test_options);
  EXPECT_TRUE(db->Execute("create table t1(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t2(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t3(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t4(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t5(id int64);").Valid());
  RandomTupleGen tuple_gen(0x202405281635);
  tuple_gen.AddInt(1, 100);
  auto [stmt_t1, data_t1] = tuple_gen.GenerateValuesClause(50);
  ASSERT_TRUE(db->Execute("insert into t1 " + stmt_t1 + ";").Valid());
  auto [stmt_t2, data_t2] = tuple_gen.GenerateValuesClause(10);
  ASSERT_TRUE(db->Execute("insert into t2 " + stmt_t2 + ";").Valid());
  auto [stmt_t3, data_t3] = tuple_gen.GenerateValuesClause(40);
  ASSERT_TRUE(db->Execute("insert into t3 " + stmt_t3 + ";").Valid());
  auto [stmt_t4, data_t4] = tuple_gen.GenerateValuesClause(20);
  ASSERT_TRUE(db->Execute("insert into t4 " + stmt_t4 + ";").Valid());
  auto [stmt_t5, data_t5] = tuple_gen.GenerateValuesClause(30);
  ASSERT_TRUE(db->Execute("insert into t5 " + stmt_t5 + ";").Valid());
  auto true_card = CalcTrueCard({"t1", "t2", "t3", "t4", "t5"}, {}, *db);
  db->SetDebugPrintPlan(true);
  db->SetEnableCostBased(true);
  db->SetTrueCardinalityHints(true_card);
  ResultSet ret;
  StopWatch sw;
  TestTimeout([&]() { ret = db->Execute("select 1 from t1, t2, t3, t4, t5;"); },
      10000, "your join is too slow!");
  DB_INFO("Used time: {}s", sw.GetTimeInSeconds());
  ASSERT_TRUE(ret.Valid());
  DB_INFO("{}, {}", ret.GetTotalOutputSize(), ret.GetPlan()->cost_);
  // standard answer: join(join(join(t2, t4), t5), join(t1, t3))
  ASSERT_EQ(ret.GetTotalOutputSize(),
      24008350);  // 12000000 * 2 + 2000 + 6000 + 200 + 10 + 20 + 30 + 40 + 50
  ASSERT_EQ(ret.GetPlan()->cost_, 12008.35);  // 12008350 * 0.001 (no hash join)
  ASSERT_EQ(true_card.back().second, ret.GetSize());
  db = nullptr;
  std::filesystem::remove_all("__tmp0209");
}

TEST(EasyOptimizerTest, Join2Tables) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0213");
  auto db = std::make_unique<wing::Instance>("__tmp0213", wing_test_options);
  EXPECT_TRUE(db->Execute("create table t1(id int64, idt3 int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t2(id int64, idt1 int64);").Valid());
  int N = 1000;
  RandomTupleGen tuple_gen(0x202405302330);
  tuple_gen.AddInt(1, 1).AddInt(1, 100);
  RandomTupleGen tuple_gen2(0x202405302338);
  tuple_gen2.AddInt(1, 100).AddInt(1, 1);
  auto [stmt_t1, data_t1] = tuple_gen.GenerateValuesClause(N);
  ASSERT_TRUE(db->Execute("insert into t1 " + stmt_t1 + ";").Valid());
  auto [stmt_t2, data_t2] = tuple_gen2.GenerateValuesClause(N);
  ASSERT_TRUE(db->Execute("insert into t2 " + stmt_t2 + ";").Valid());
  auto true_card = CalcTrueCard(
      {"t1", "t2"}, {PredElement("t1.id = t2.idt1", {"t1", "t2"})}, *db);
  db->SetDebugPrintPlan(true);
  db->SetEnableCostBased(true);
  db->SetTrueCardinalityHints(true_card);
  auto ret = db->Execute("select 1 from t1, t2 where t1.id = t2.idt1;");
  ASSERT_TRUE(ret.Valid());
  DB_INFO("{}, {}", ret.GetTotalOutputSize(), ret.GetPlan()->cost_);
  // standard answer: nested loop join(t1, t2)
  // hash join cost: (1000 + 1000) * 0.01 + 1e6 * 0.001 = 1020
  // nested loop join cost: 1e6 * 0.001 = 1000
  // plus sequential scan cost (1000 + 1000) * 0.001 = 2.
  ASSERT_EQ(ret.GetTotalOutputSize(), 2002000);
  ASSERT_EQ(ret.GetPlan()->cost_, 1002);
  ASSERT_EQ(true_card.back().second, ret.GetSize());
  db = nullptr;
  std::filesystem::remove_all("__tmp0213");
}

wing::ResultSet ExecuteSimpleSQLWithTrueCard(
    const std::vector<PredElement>& preds,
    const std::vector<std::string>& tables, const std::string& cache_name,
    wing::Instance& db) {
  std::string pred_str;
  for (auto& pred : preds) {
    if (!pred_str.empty()) {
      pred_str += " and ";
    }
    pred_str += pred.pred_str;
  }
  std::string table_str;
  for (auto& table : tables) {
    if (!table_str.empty()) {
      table_str += ", ";
    }
    table_str += table;
  }
  auto cached_true_card = ReadTrueCard(cache_name);
  auto true_card = cached_true_card ? cached_true_card.value()
                                    : CalcTrueCard(tables, preds, db);
  if (!cached_true_card)
    PrintTrueCard(true_card, cache_name);
  db.SetTrueCardinalityHints(true_card);
  db.SetDebugPrintPlan(true);
  db.SetEnableCostBased(true);
  wing::ResultSet result;
  wing::StopWatch sw;
  wing::wing_testing::TestTimeout(
      [&]() {
        result = db.Execute(
            fmt::format("select 1 from {} where {};", table_str, pred_str));
      },
      7000, "your join and optimizer is too slow!! ");
  DB_INFO("Used time: {}s", sw.GetTimeInSeconds());
  {
    auto tables1 = tables;
    auto true_size = -1e200;
    std::sort(tables1.begin(), tables1.end());
    for (auto& [tables2, size] : true_card) {
      std::sort(tables2.begin(), tables2.end());
      if (tables2 == tables1) {
        true_size = size;
        break;
      }
    }
    auto result_size = result.GetSize();
    DB_ASSERT(true_size == result_size);
  }
  return result;
}

TEST(EasyOptimizerTest, Join11Tables) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0210");
  auto db = std::make_unique<wing::Instance>("__tmp0210", wing_test_options);
  EXPECT_TRUE(db->Execute("create table t1(id12 int64);").Valid());
  EXPECT_TRUE(
      db->Execute("create table t2(id12 int64, id23 int64, id27 int64);")
          .Valid());
  EXPECT_TRUE(db->Execute("create table t3(id23 int64, id3X int64);").Valid());
  EXPECT_TRUE(
      db->Execute("create table t4(id48 int64, id45 int64, id4X1 int64);")
          .Valid());
  EXPECT_TRUE(
      db->Execute("create table t5(id59 int64, id56 int64, id45 int64);")
          .Valid());
  EXPECT_TRUE(
      db->Execute("create table t6(id56 int64, id68 int64, id67 int64);")
          .Valid());
  EXPECT_TRUE(
      db->Execute("create table t7(id67 int64, id27 int64, id79 int64);")
          .Valid());
  EXPECT_TRUE(db->Execute("create table t8(id48 int64, id68 int64);").Valid());
  EXPECT_TRUE(
      db->Execute("create table t9(id59 int64, id79 int64, id9X int64);")
          .Valid());
  EXPECT_TRUE(db->Execute("create table t10(id9X int64, id3X int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t11(id4X1 int64);").Valid());

  std::vector<size_t> sizes = {
      50, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100};
  std::vector<size_t> col_num = {1, 3, 2, 3, 3, 3, 3, 2, 3, 2, 1};
  std::vector<std::string> tablename = {
      "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11"};
  for (uint32_t i = 0; i < tablename.size(); i++) {
    RandomTupleGen tuple_gen(0x20240529145900 + i);
    for (uint32_t j = 0; j < col_num[i]; j++) {
      tuple_gen.AddInt(1, 20);
    }
    auto [stmt, data] = tuple_gen.GenerateValuesClause(sizes[i]);
    ASSERT_TRUE(
        db->Execute("insert into " + tablename[i] + " " + stmt + ";").Valid());
    db->Analyze(tablename[i]);
  }

  // test 1 all join
  {
    auto preds = std::vector<PredElement>{
        PredElement("t5.id59 = t9.id59", {"t5", "t9"}),
        PredElement("t7.id79 = t9.id79", {"t7", "t9"}),
        PredElement("t10.id9X = t9.id9X", {"t10", "t9"}),
        PredElement("t5.id45 = t4.id45", {"t4", "t5"}),
        PredElement("t5.id56 = t6.id56", {"t5", "t6"}),
        PredElement("t6.id68 = t8.id68", {"t6", "t8"}),
        PredElement("t6.id67 = t7.id67", {"t6", "t7"}),
        PredElement("t4.id48 = t8.id48", {"t4", "t8"}),
        PredElement("t2.id27 = t7.id27", {"t2", "t7"}),
        PredElement("t2.id12 = t1.id12", {"t1", "t2"}),
        PredElement("t2.id23 = t3.id23", {"t2", "t3"}),
        PredElement("t3.id3X = t10.id3X", {"t3", "t10"}),
        PredElement("t4.id4X1 = t11.id4X1", {"t4", "t11"}),
    };
    auto ret = ExecuteSimpleSQLWithTrueCard(preds,
        {"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11"},
        "test/true_card_hints/join11tables_cache.txt", *db);
    ASSERT_TRUE(ret.Valid());
    DB_INFO("{}, {}", ret.GetTotalOutputSize(), ret.GetPlan()->cost_);
    ASSERT_EQ(ret.GetTotalOutputSize(), 136368);
    ASSERT_TRUE(fabs(ret.GetPlan()->cost_ - 231.69) < 1e-9);
  }
  // test 2 join a chain {t11, t4, t8, t6, t7, t2, t3, t10}
  {
    auto preds = std::vector<PredElement>{
        PredElement("t3.id3X = t10.id3X", {"t3", "t10"}),
        PredElement("t4.id4X1 = t11.id4X1", {"t4", "t11"}),
        PredElement("t4.id48 = t8.id48", {"t4", "t8"}),
        PredElement("t2.id27 = t7.id27", {"t2", "t7"}),
        PredElement("t6.id68 = t8.id68", {"t6", "t8"}),
        PredElement("t6.id67 = t7.id67", {"t6", "t7"}),
        PredElement("t2.id23 = t3.id23", {"t2", "t3"}),
    };
    auto ret = ExecuteSimpleSQLWithTrueCard(preds,
        {"t2", "t3", "t4", "t6", "t7", "t8", "t10", "t11"},
        "test/true_card_hints/join11tables_cache.txt", *db);
    ASSERT_TRUE(ret.Valid());
    DB_INFO("{}, {}", ret.GetTotalOutputSize(), ret.GetPlan()->cost_);
    ASSERT_EQ(ret.GetTotalOutputSize(), 15128676);
    ASSERT_TRUE(fabs(ret.GetPlan()->cost_ - 7854.474) < 1e-9);
  }
  // test 3 join a cycle {}
  {
    auto preds = std::vector<PredElement>{
        PredElement("t3.id3X = t10.id3X", {"t3", "t10"}),
        PredElement("t2.id23 = t3.id23", {"t2", "t3"}),
        PredElement("t2.id27 = t7.id27", {"t2", "t7"}),
        PredElement("t6.id67 = t7.id67", {"t6", "t7"}),
        PredElement("t5.id56 = t6.id56", {"t5", "t6"}),
        PredElement("t5.id59 = t9.id59", {"t5", "t9"}),
        PredElement("t7.id79 = t9.id79", {"t7", "t9"}),
        PredElement("t10.id9X = t9.id9X", {"t10", "t9"}),
    };
    auto ret = ExecuteSimpleSQLWithTrueCard(preds,
        {"t5", "t6", "t7", "t2", "t3", "t10", "t9"},
        "test/true_card_hints/join11tables_cache.txt", *db);
    ASSERT_TRUE(ret.Valid());
    DB_INFO("{}, {}", ret.GetTotalOutputSize(), ret.GetPlan()->cost_);
    ASSERT_EQ(ret.GetTotalOutputSize(), 12863);
    ASSERT_TRUE(fabs(ret.GetPlan()->cost_ - 61.966) < 1e-9);
  }
  db = nullptr;
  std::filesystem::remove_all("__tmp0210");
}

TEST(EasyOptimizerTest, Join15TableCluster) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0212");
  auto db = std::make_unique<wing::Instance>("__tmp0212", wing_test_options);
  EXPECT_TRUE(db->Execute("create table t1(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t2(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t3(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t4(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t5(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t6(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t7(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t8(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t9(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t10(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t11(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t12(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t13(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t14(id int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t15(id int64);").Valid());
  std::vector<size_t> sizes = {100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
      100, 100, 100, 100, 100};
  std::vector<size_t> col_num = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
  std::vector<std::string> tablename = {"t1", "t2", "t3", "t4", "t5", "t6",
      "t7", "t8", "t9", "t10", "t11", "t12", "t13", "t14", "t15"};
  for (uint32_t i = 0; i < tablename.size(); i++) {
    RandomTupleGen tuple_gen(0x202405302159 + i);
    for (uint32_t j = 0; j < col_num[i]; j++) {
      tuple_gen.AddInt(1, 50);
    }
    auto [stmt, data] = tuple_gen.GenerateValuesClause(sizes[i]);
    ASSERT_TRUE(
        db->Execute("insert into " + tablename[i] + " " + stmt + ";").Valid());
    db->Analyze(tablename[i]);
  }
  // test 1 cluster
  {
    auto preds = std::vector<PredElement>();
    for (auto L : tablename)
      for (auto R : tablename)
        if (L < R) {
          preds.emplace_back(fmt::format("{}.id = {}.id", L, R),
              std::vector<std::string>{L, R});
        }
    auto ret = ExecuteSimpleSQLWithTrueCard(preds,
        {"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11",
            "t12", "t13", "t14", "t15"},
        "test/true_card_hints/join15tablecluster_cache.txt", *db);
    ASSERT_TRUE(ret.Valid());
    DB_INFO("{}, {}", ret.GetTotalOutputSize(), ret.GetPlan()->cost_);
    ASSERT_EQ(ret.GetTotalOutputSize(), 1594912);
    ASSERT_TRUE(fabs(ret.GetPlan()->cost_ - 902.96) < 1e-9);
  }
  db = nullptr;
  std::filesystem::remove_all("__tmp0212");
}

TEST(OptimizerTest, PredTransTable2Small) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0216");
  auto db = std::make_unique<wing::Instance>("__tmp0216", wing_test_options);
  db->SetEnablePredTrans(true);
  db->SetEnableCostBased(false);
  EXPECT_TRUE(db->Execute("create table t1(id int64, idt2 int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t2(id int64, idt1 int64);").Valid());
  {
    ASSERT_TRUE(db->Execute("insert into t1 values(3, 10), (4, 11);").Valid());
    ASSERT_TRUE(db->Execute("insert into t2 values(1, 2), (2, 3);").Valid());
    // join result is (3, 10) (t1) - (2, 3) (t2)
    auto result = db->Execute("select 1 from t1, t2 where t1.id = t2.idt1;");
    DB_INFO("{}", result.GetTotalOutputSize());
    ASSERT_EQ(result.GetTotalOutputSize(), 1 + 1 + 1 * 2);
    ASSERT_TRUE(db->Execute("delete from t1;").Valid());
    ASSERT_TRUE(db->Execute("delete from t2;").Valid());
    ASSERT_EQ(db->Execute("select 1 from t1, t2;").GetTotalOutputSize(), 0);
  }

  {
    ASSERT_TRUE(
        db->Execute("insert into t1 values(3, 10), (4, 11), (5, 13);").Valid());
    ASSERT_TRUE(
        db->Execute("insert into t2 values(13, 2), (12, 3), (11, 4);").Valid());
    // join result is (4, 11) (t1) - (11, 4) (t2)
    auto result = db->Execute(
        "select 1 from t1, t2 where t1.id = t2.idt1 and t2.id = t1.idt2;");
    DB_INFO("{}", result.GetTotalOutputSize());
    ASSERT_EQ(result.GetTotalOutputSize(), 1 + 1 + 1 * 2);
    ASSERT_TRUE(db->Execute("delete from t1;").Valid());
    ASSERT_TRUE(db->Execute("delete from t2;").Valid());
    ASSERT_EQ(db->Execute("select 1 from t1, t2;").GetTotalOutputSize(), 0);
  }
  db = nullptr;
  std::filesystem::remove_all("__tmp0216");
}

TEST(OptimizerTest, PredTransTable2Big) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0214");
  auto db = std::make_unique<wing::Instance>("__tmp0214", wing_test_options);
  db->SetEnablePredTrans(true);
  db->SetEnableCostBased(false);
  EXPECT_TRUE(db->Execute("create table t1(id int64, idt2 int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t2(id int64, idt1 int64);").Valid());
  int N = 3000;
  {
    // used tuples in joining two tables.
    RandomTupleGen tuple_gen(0x20240531215500);
    tuple_gen.AddInt(1, 1).AddInt(1, 100);
    auto [stmt_t1, data_t1] = tuple_gen.GenerateValuesClause(N / 2);
    ASSERT_TRUE(db->Execute("insert into t1 " + stmt_t1 + ";").Valid());
  }

  {
    // dangling tuples in joining two tables.
    RandomTupleGen tuple_gen(0x20240531215501);
    tuple_gen.AddInt(11, 100).AddInt(1, 100);
    auto [stmt_t1, data_t1] = tuple_gen.GenerateValuesClause(N / 2);
    ASSERT_TRUE(db->Execute("insert into t1 " + stmt_t1 + ";").Valid());
  }

  {
    // used tuples in joining two tables.
    RandomTupleGen tuple_gen2(0x20240531215502);
    tuple_gen2.AddInt(1, 100).AddInt(1, 1);
    auto [stmt_t2, data_t2] = tuple_gen2.GenerateValuesClause(N / 2);
    ASSERT_TRUE(db->Execute("insert into t2 " + stmt_t2 + ";").Valid());
  }

  {
    // dangling tuples in joining two tables.
    RandomTupleGen tuple_gen2(0x20240531215503);
    tuple_gen2.AddInt(1, 100).AddInt(2, 10);
    auto [stmt_t2, data_t2] = tuple_gen2.GenerateValuesClause(N / 2);
    ASSERT_TRUE(db->Execute("insert into t2 " + stmt_t2 + ";").Valid());
  }

  auto result = db->Execute("select 1 from t1, t2 where t1.id = t2.idt1;");
  ASSERT_TRUE(result.Valid());
  DB_INFO("{}", result.GetTotalOutputSize());
  ASSERT_EQ(result.GetTotalOutputSize(), (N / 2) * (N / 2) * 2 + (N / 2) * 2);

  db = nullptr;
  std::filesystem::remove_all("__tmp0214");
}

TEST(OptimizerTest, PredTransTable3Small) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0215");
  auto db = std::make_unique<wing::Instance>("__tmp0215", wing_test_options);
  db->SetEnablePredTrans(true);
  db->SetEnableCostBased(false);
  EXPECT_TRUE(
      db->Execute("create table t1(a int64, b int64, c int64);").Valid());
  EXPECT_TRUE(
      db->Execute("create table t2(b int64, c int64, d int64);").Valid());
  EXPECT_TRUE(
      db->Execute("create table t3(c int64, d int64, e int64);").Valid());
  // output is (x, x, 5, x, x) where x = 1, 2, 3, 4.
  ASSERT_TRUE(
      db->Execute("insert into t1 values(1, 1, 5), (2, 2, 5), (3, 3, 5), (4, "
                  "4, 5), (114514, 5, 5), (114515, 6, 5), (114516, 8, 5);")
          .Valid());
  ASSERT_TRUE(db->Execute("insert into t2 values(1, 5, 1), (2, 5, 2), (3, 5, "
                          "3), (4, 5, 4), (6, 5, 6), (7, 5, 7);")
                  .Valid());
  ASSERT_TRUE(db->Execute("insert into t3 values(5, 1, 1), (5, 2, 2), (5, 3, "
                          "3), (5, 4, 4), (5, 5, 114514), (8, 5, 114515);")
                  .Valid());
  auto result = db->Execute(
      "select 1 from t1, t2, t3 where t1.b = t2.b and t1.c = t2.c and t2.c = "
      "t3.c and t2.d = t3.d;");
  ASSERT_TRUE(result.Valid());
  DB_INFO("{}", result.GetTotalOutputSize());
  ASSERT_EQ(result.GetTotalOutputSize(), 24);

  db = nullptr;
  std::filesystem::remove_all("__tmp0215");
}

TEST(OptimizerTest, PredTransTable3Big) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0217");
  auto db = std::make_unique<wing::Instance>("__tmp0217", wing_test_options);
  db->SetEnablePredTrans(true);
  db->SetEnableCostBased(false);
  EXPECT_TRUE(db->Execute("create table t1(a int64, b varchar(5));").Valid());
  EXPECT_TRUE(db->Execute("create table t2(a int64, c int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t3(b varchar(5), d int64);").Valid());
  int N = 6000;

  // t1
  RandomTupleGen tuple_gen1(0x20240601004200);
  tuple_gen1.AddInt(1, 10000).AddString(1, 4);
  auto [stmt_t1, data_t1] = tuple_gen1.GenerateValuesClause(N);
  ASSERT_TRUE(db->Execute("insert into t1 " + stmt_t1 + ";").Valid());

  // t2
  RandomTupleGen tuple_gen2(0x20240601004201);
  tuple_gen2.AddInt(1, 10000).AddInt(1, 10000);
  auto [stmt_t2, data_t2] = tuple_gen2.GenerateValuesClause(N);
  ASSERT_TRUE(db->Execute("insert into t2 " + stmt_t2 + ";").Valid());

  // t3
  RandomTupleGen tuple_gen3(0x20240601004202);
  tuple_gen3.AddString(1, 4).AddInt(1, 10000);
  auto [stmt_t3, data_t3] = tuple_gen3.GenerateValuesClause(N);
  ASSERT_TRUE(db->Execute("insert into t3 " + stmt_t3 + ";").Valid());

  std::set<size_t> Aset;
  std::set<std::string> Bset;

  size_t size_t123 = 0;
  size_t size_t1 = 0, size_t12 = 0;
  for (int i = 0; i < N; i++) {
    bool flag_t1 = false;
    for (int j = 0; j < N; j++)
      if (data_t1.Get(i, 0).ReadInt() == data_t2.Get(j, 0).ReadInt()) {
        bool flag = false;
        for (int k = 0; k < N; k++)
          if (data_t1.Get(i, 1).ReadString() ==
              data_t3.Get(k, 0).ReadString()) {
            size_t123 += 1;
            if (!flag) {
              size_t12 += 1;
              Aset.insert(data_t1.Get(i, 0).ReadInt());
              Bset.insert(std::string(data_t1.Get(i, 1).ReadString()));
            }
            flag = true;
            flag_t1 = true;
          }
      }
    if (flag_t1) {
      size_t1 += 1;
    }
  }

  size_t size_t2 = 0, size_t3 = 0;
  for (uint32_t i = 0; i < N; i++) {
    size_t2 += Aset.count(data_t2.Get(i, 0).ReadInt());
    size_t3 += Bset.count(std::string(data_t3.Get(i, 0).ReadString()));
  }
  DB_INFO("{}, {}, {}, {}, {}", size_t123, size_t12, size_t1, size_t2, size_t3);
  DB_INFO("standard answer of total output: {}",
      size_t123 * 2 + size_t12 + size_t1 + size_t2 + size_t3);

  auto result = db->Execute(
      "select 1 from (t1 join t2 on t1.a = t2.a) join t3 on t1.b = t3.b;");
  ASSERT_TRUE(result.Valid());
  DB_INFO("{}, {}", result.GetTotalOutputSize(), result.GetSize());
  ASSERT_TRUE(result.GetTotalOutputSize() <=
              size_t123 * 2 + size_t12 + size_t1 + size_t2 + size_t3 + 30);
  ASSERT_EQ(result.GetSize(), size_t123);

  db = nullptr;
  std::filesystem::remove_all("__tmp0217");
}

TEST(OptimizerTest, PredTransTree) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0218");
  auto db = std::make_unique<wing::Instance>("__tmp0218", wing_test_options);
  db->SetEnablePredTrans(true);
  db->SetEnableCostBased(false);
  EXPECT_TRUE(
      db->Execute("create table t1(r12 int64, r13 int64, r1X int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t2(r12 int64, r27 int64);").Valid());
  EXPECT_TRUE(
      db->Execute(
            "create table t3(r13 int64, r34 int64, r35 int64, r36 int64);")
          .Valid());
  EXPECT_TRUE(db->Execute("create table t4(r34 int64);").Valid());
  EXPECT_TRUE(
      db->Execute("create table t5(r35 int64, r58 int64, r59 int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t6(r36 int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t7(r27 int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t8(r58 int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t9(r59 int64);").Valid());
  EXPECT_TRUE(db->Execute("create table t10(r1X int64);").Valid());
  std::vector<size_t> num_cols = {3, 2, 4, 1, 3, 1, 1, 1, 1, 1};
  std::vector<std::string> tablename = {
      "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10"};
  int N = 20000;
  std::mt19937_64 rgen(0x202406050141);
  for (int i = 0; i < tablename.size(); i++) {
    auto table = tablename[i];
    auto num_col = num_cols[i];
    RandomTupleGen tuple_gen(0x20240605013700 + i * 1926);
    RandomTupleGen tuple_gen2(0x20240605013700 + i * 817);
    for (uint32_t j = 0; j < num_col; j++) {
      tuple_gen.AddInt(10000, 1e9);
      tuple_gen2.AddInt(1, 1000);
    }
    for (uint32_t j = 0; j < N; j++) {
      auto [stmt, data] = rgen() % 160 < 8 ? tuple_gen2.GenerateValuesClause(1)
                                           : tuple_gen.GenerateValuesClause(1);
      ASSERT_TRUE(
          db->Execute("insert into " + table + " " + stmt + ";").Valid());
    }
  }

  ResultSet result;
  StopWatch sw;
  TestTimeout(
      [&]() {
        result = db->Execute(
            "select 1 from t1, t2, t3, t4, t5, t6, t7, t8, t9, t10 where "
            "t1.r12 = t2.r12 and t1.r13 = t3.r13 and t1.r1X = t10.r1X and "
            "t2.r27 = t7.r27 and "
            "t3.r34 = t4.r34 and t3.r35 = t5.r35 and t3.r36 = t6.r36 and "
            "t5.r58 = t8.r58 and t5.r59 = t9.r59;");
      },
      10000, "your join is too slow!");
  DB_INFO("Used time: {}s", sw.GetTimeInSeconds());
  ASSERT_TRUE(result.Valid());
  DB_INFO("{}, {}", result.GetTotalOutputSize(), result.GetSize());
  ASSERT_EQ(result.GetSize(), 1122);
  /* The standard program output 4926. You should be in 4926 + eps */
  ASSERT_TRUE(result.GetTotalOutputSize() <= 6000);

  sw.Reset();
  TestTimeout(
      [&]() {
        result = db->Execute(
            "select 1 from t1, t2, t3, t4, t5, t6, t7, t8, t9, t10 where "
            "t1.r12 = t2.r12 and t1.r13 = t3.r13 and t1.r1X = t10.r1X and "
            "t2.r27 = t7.r27 and "
            "t3.r34 = t4.r34 and t3.r35 = t5.r35 and t3.r36 = t6.r36 and "
            "t5.r58 = t8.r58 and t5.r59 = t9.r59 and "
            "t1.r12 <= 100 and t1.r13 <= 100 and t10.r1X <= 100;");
      },
      10000, "your join is too slow!");
  DB_INFO("Used time: {}s", sw.GetTimeInSeconds());
  ASSERT_TRUE(result.Valid());
  DB_INFO("{}, {}", result.GetTotalOutputSize(), result.GetSize());
  ASSERT_EQ(result.GetSize(), 0);
  /* The standard program output 0. You should be in 0 + eps */
  ASSERT_TRUE(result.GetTotalOutputSize() <= 250);

  db = nullptr;
  std::filesystem::remove_all("__tmp0218");
}

TEST(OptimizerTest, PredTransStar) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0219");
  auto db = std::make_unique<wing::Instance>("__tmp0219", wing_test_options);
  db->SetEnablePredTrans(true);
  db->SetEnableCostBased(false);
  int tableN = 100;
  for (int i = 0; i < tableN; i++) {
    EXPECT_TRUE(
        db->Execute(fmt::format("create table t{}(id int64);", i)).Valid());
  }
  {
    std::string table_str = "";
    for (int i = 0; i < tableN; i++) {
      table_str += ", id" + std::to_string(i) + " int64";
    }
    EXPECT_TRUE(
        db->Execute(fmt::format("create table main(a int64 {});", table_str))
            .Valid());
  }
  int N = 1000;
  int mainN = 1e5;
  RandomTupleGen tuple_gen(0x202406050223);
  tuple_gen.AddInt(1, 10);
  RandomTupleGen tuple_gen2(0x202406050223);
  tuple_gen2.AddInt();
  for (int i = 0; i < tableN; i++) {
    if (i == tableN / 2) {
      auto [stmt, data] = tuple_gen2.GenerateValuesClause(N / 2);
      ASSERT_TRUE(
          db->Execute(fmt::format("insert into t{} {};", i, stmt)).Valid());
      ASSERT_TRUE(
          db->Execute(fmt::format("insert into t{} values (114514);", i))
              .Valid());
      auto [stmt2, data2] = tuple_gen2.GenerateValuesClause(N / 2);
      ASSERT_TRUE(
          db->Execute(fmt::format("insert into t{} {};", i, stmt2)).Valid());
    } else {
      auto [stmt, data] = tuple_gen.GenerateValuesClause(N / 2);
      ASSERT_TRUE(
          db->Execute(fmt::format("insert into t{} {};", i, stmt)).Valid());
      ASSERT_TRUE(
          db->Execute(fmt::format("insert into t{} values (114514);", i))
              .Valid());
      auto [stmt2, data2] = tuple_gen.GenerateValuesClause(N / 2);
      ASSERT_TRUE(
          db->Execute(fmt::format("insert into t{} {};", i, stmt2)).Valid());
    }
  }

  {
    RandomTupleGen tuple_gen_main(0x202406050226);
    tuple_gen_main.AddInt(1, 10);
    std::string values_str = "114514";
    for (int i = 0; i < tableN; i++) {
      tuple_gen_main.AddInt(1, 10);
      values_str += ", 114514";
    }
    auto [stmt, data] = tuple_gen_main.GenerateValuesClause(mainN / 2);
    ASSERT_TRUE(db->Execute(fmt::format("insert into main {};", stmt)).Valid());
    ASSERT_TRUE(
        db->Execute(fmt::format("insert into main values ({});", values_str))
            .Valid());
    auto [stmt2, data2] = tuple_gen_main.GenerateValuesClause(mainN / 2);
    ASSERT_TRUE(
        db->Execute(fmt::format("insert into main {};", stmt2)).Valid());
  }

  {
    std::string sql_str = "select 1 from main";
    for (int i = 0; i < tableN; i++) {
      sql_str += fmt::format(", t{}", i);
    }
    sql_str += " where 1";
    for (int i = 0; i < tableN; i++) {
      sql_str += fmt::format(" and t{}.id = main.id{}", i, i);
    }
    sql_str += ";";

    ResultSet result;
    StopWatch sw;
    TestTimeout([&]() { result = db->Execute(sql_str); }, 3000,
        "your predicate transfer is too slow!");
    DB_INFO("Used time: {}s", sw.GetTimeInSeconds());
    ASSERT_TRUE(result.Valid());
    DB_INFO("{}, {}", result.GetTotalOutputSize(), result.GetSize());
    /* The answer is (114514, 114514. ...)*/
    ASSERT_EQ(result.GetSize(), 1);
    /* The standard program output 205. You should be in 205 + eps */
    ASSERT_TRUE(result.GetTotalOutputSize() <= 500);
  }

  db = nullptr;
  std::filesystem::remove_all("__tmp0219");
}

TEST(OptimizerTest, PredTransCluster) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove_all("__tmp0220");
  auto db = std::make_unique<wing::Instance>("__tmp0220", wing_test_options);
  db->SetEnablePredTrans(true);
  db->SetEnableCostBased(false);
  int tableN = 40;
  for (int i = 1; i <= tableN; i++) {
    EXPECT_TRUE(
        db->Execute(fmt::format("create table t{}(id int64);", i)).Valid());
  }
  int N = 10000;
  RandomTupleGen tuple_gen(0x202406050250);
  tuple_gen.AddInt(1, N * 0.72);
  for (int i = 1; i <= tableN; i++) {
    auto [stmt, data] = tuple_gen.GenerateValuesClause(N);
    ASSERT_TRUE(
        db->Execute(fmt::format("insert into t{} {};", i, stmt)).Valid());
  }
  // test 1 cluster
  {
    std::string sql_str = "select 1 from t1";
    for (int i = 2; i <= tableN; i++) {
      sql_str += fmt::format(", t{}", i);
    }
    sql_str += " where 1";
    for (int i = 1; i <= tableN; i++)
      for (int j = i + 1; j <= tableN; j++) {
        sql_str += fmt::format(" and t{}.id = t{}.id", i, j);
      }
    sql_str += ";";
    ResultSet result;
    StopWatch sw;
    TestTimeout([&]() { result = db->Execute(sql_str); }, 2000,
        "your predicate transfer is too slow!");
    DB_INFO("Used time: {}s", sw.GetTimeInSeconds());
    ASSERT_TRUE(result.Valid());
    DB_INFO("{}, {}", result.GetTotalOutputSize(), result.GetSize());
    ASSERT_EQ(result.GetSize(), 0);
    /* The standard program output 0. You should be in 0 + eps */
    ASSERT_TRUE(result.GetTotalOutputSize() <= 250);
  }

  {
    std::string sql_str = "select 1 from t1";
    for (int i = 2; i <= tableN / 4; i++) {
      sql_str += fmt::format(", t{}", i);
    }
    sql_str += " where 1";
    for (int i = 1; i <= tableN / 4; i++) {
      for (int j = i + 1; j <= tableN / 4; j++) {
        sql_str += fmt::format(" and t{}.id = t{}.id", i, j);
      }
      sql_str += fmt::format(" and t{}.id <= {};", i, N / 10);
    }
    sql_str += ";";
    ResultSet result;
    StopWatch sw;
    TestTimeout([&]() { result = db->Execute(sql_str); }, 2000,
        "your predicate transfer is too slow!");
    DB_INFO("Used time: {}s", sw.GetTimeInSeconds());
    ASSERT_TRUE(result.Valid());
    DB_INFO("{}, {}", result.GetTotalOutputSize(), result.GetSize());
    ASSERT_EQ(result.GetSize(), 16216);
    /* The output of standard program is 57231. <= 57231 + eps */
    ASSERT_TRUE(result.GetTotalOutputSize() <= 57500);
  }
  db = nullptr;
  std::filesystem::remove_all("__tmp0220");
}

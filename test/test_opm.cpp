#include <gtest/gtest.h>

#include <filesystem>

#include "catalog/stat.hpp"
#include "common/stopwatch.hpp"
#include "instance/instance.hpp"
#include "test.hpp"
#include "zipf.hpp"

TEST(OptimizerTest, PushdownTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0201");
  auto db = std::make_unique<wing::Instance>("__tmp0201", SAKURA_USE_JIT_FLAG);
  {
    ResultSet result;
    EXPECT_TRUE(test_timeout(
        [&]() {
          // clang-format off
          result = db->Execute("select * from (values(1),(2),(3),(4),(5))A(a),(values(1),(2),(3),(4),(5))B(a),(values(1),(2),(3),(4),(5))C(a),(values(1),(2),(3),(4),(5))D(a),(values(1),(2),(3),(4),(5))E(a),(values(1),(2),(3),(4),(5))F(a),(values(1),(2),(3),(4),(5))G(a),(values(1),(2),(3),(4),(5))H(a),(values(1),(2),(3),(4),(5))I(a),(values(1),(2),(3),(4),(5))J(a),(select * from (select * from (values(1),(2),(3),(4),(5))K(a),(values(1),(2),(3),(4),(5))L(a),(values(1),(2),(3),(4),(5))M(a),(values(1),(2),(3),(4),(5))N(a)),(select * from (values(1),(2),(3),(4),(5))O(a),(values(1),(2),(3),(4),(5))P(a),(values(1),(2),(3),(4),(5))Q(a))),(values(1),(2),(3),(4),(5))R(a),(values(1),(2),(3),(4),(5))S(a),(values(1),(2),(3),(4),(5))T(a),(values(1),(2),(3),(4),(5))U(a),(values(1),(2),(3),(4),(5))V(a),(values(1),(2),(3),(4),(5))W(a),(values(1),(2),(3),(4),(5))X(a),(values(1),(2),(3),(4),(5))Y(a),(values(1),(2),(3),(4),(5))Z(a)where A.a*2<6+A.a and 4<A.a and 1<>A.a and B.a=4 and C.a=2 and D.a=5 and E.a=2 and F.a=4 and G.a+1=3 and H.a=2 and I.a*2=4 and J.a=1 and K.a=K.a*2-4 and 1=L.a and M.a=5 and N.a=4 and O.a=3 and P.a>0 and P.a<2 and Q.a=1 and R.a=1 and S.a>2 and S.a<4 and T.a>4 and U.a=3 and V.a=1 and W.a=3 and X.a=5+X.a-X.a and Y.a=1 and Z.a=5 and 1=1;");
          // clang-format on
        },
        1000));
    EXPECT_TRUE(result.Valid());
    AnsVec answer;
    answer.push_back(MkVec(IV::Create(5), IV::Create(4), IV::Create(2),
        IV::Create(5), IV::Create(2), IV::Create(4), IV::Create(2),
        IV::Create(2), IV::Create(2), IV::Create(1), IV::Create(4),
        IV::Create(1), IV::Create(5), IV::Create(4), IV::Create(3),
        IV::Create(1), IV::Create(1), IV::Create(1), IV::Create(3),
        IV::Create(5), IV::Create(3), IV::Create(1), IV::Create(3),
        IV::Create(5), IV::Create(1), IV::Create(5)));
    CHECK_ALL_SORTED_ANS(answer, result, 26);
  }

  {
    EXPECT_TRUE(
        db->Execute("create table A(a int64 primary key, b int64);").Valid());
    EXPECT_TRUE(
        db->Execute("create table B(a int64 primary key, b int64);").Valid());
    EXPECT_TRUE(
        db->Execute("create table C(a int64 primary key, b int64);").Valid());
    RandomTuple<int64_t, int64_t> tuple_gen(
        202303041622ull, INT64_MIN, INT64_MAX, 0, 100);
    int NUM = 1e4;
    auto [stmt_a, data_a] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into A " + stmt_a + ";").Valid());
    auto [stmt_b, data_b] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into B " + stmt_b + ";").Valid());
    auto [stmt_c, data_c] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into C " + stmt_c + ";").Valid());
    ResultSet result;
    EXPECT_TRUE(test_timeout(
        [&]() {
          // clang-format off
          result = db->Execute("select A.a, B.a, C.a from A, B, C where ((A.a ^ B.a ^ C.a) & 6) = 6 and A.b = 0 and B.b = 0 and C.b = 0 order by A.a asc, B.a asc, C.a asc;");
          // clang-format on
        },
        1000));
    std::vector<int64_t> A, B, C;
    for (int i = 0; i < NUM; i++)
      if (data_a.Get(i, 1)->ReadInt() == 0)
        A.push_back(data_a.Get(i, 0)->ReadInt());
    for (int i = 0; i < NUM; i++)
      if (data_b.Get(i, 1)->ReadInt() == 0)
        B.push_back(data_b.Get(i, 0)->ReadInt());
    for (int i = 0; i < NUM; i++)
      if (data_c.Get(i, 1)->ReadInt() == 0)
        C.push_back(data_c.Get(i, 0)->ReadInt());
    std::sort(A.begin(), A.end());
    std::sort(B.begin(), B.end());
    std::sort(C.begin(), C.end());
    AnsVec answer;
    for (auto a : A)
      for (auto b : B)
        for (auto c : C)
          if (((a ^ b ^ c) & 6) == 6) {
            answer.push_back(
                MkVec(IV::Create(a), IV::Create(b), IV::Create(c)));
          }
    CHECK_ALL_SORTED_ANS(answer, result, 3);
  }

  db = nullptr;
  std::filesystem::remove("__tmp0201");
}

// TEST(OptimizerTest, ProjectFoldTest) {
//   using namespace wing;
//   using namespace wing::wing_testing;
//   std::filesystem::remove("__tmp0202");
//   auto db = std::make_unique<wing::Instance>("__tmp0202",
//   SAKURA_USE_JIT_FLAG);
//   {
//     EXPECT_TRUE(
//         db->Execute("create table A(a int64 primary key, b int64, c
//         float64);")
//             .Valid());
//     RandomTuple<int64_t, int64_t, double> tuple_gen(
//         202303051133ull, INT64_MIN, INT64_MAX, 0, 100, 0.0, 1.0);
//     int NUM = 5e3;
//     auto [stmt_a, data_a] = tuple_gen.GenerateValuesClause(NUM);
//     EXPECT_TRUE(db->Execute("insert into A " + stmt_a + ";").Valid());
//     StopWatch sw;
//     EXPECT_TRUE(test_timeout(
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
//         4000));
//     DB_INFO("Use: {} s", sw.GetTimeInSeconds());
//   }
//   db = nullptr;
//   std::filesystem::remove("__tmp0202");
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
  std::filesystem::remove("__tmp0203");
  auto db = std::make_unique<wing::Instance>("__tmp0203", SAKURA_USE_JIT_FLAG);
  {
    EXPECT_TRUE(db->Execute("create table B(a int64 auto_increment primary "
                            "key, data varchar(2));")
                    .Valid());
    EXPECT_TRUE(db->Execute("create table A(a int64 foreign key references "
                            "B(a), b int64, c float64);")
                    .Valid());
    int NUMA = 1e6, NUMB = 1e3;
    RandomTuple<int64_t, int64_t, double> tuple_gen(
        202303051627ull, 1, NUMB, 0, 10000, 0.0, 1.0);
    DB_INFO("Generating data...");
    StopWatch _sw;
    RandomTuple<int64_t, std::string> tuple_gen2(
        20230305162702ull, 0, 0, 1, 2);
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

    // EXPECT_TRUE(test_timeout(
    //     [&]() {
    //       // clang-format off
    //       db->Execute("select * from A, B where A.a = B.a;");
    //       // clang-format on
    //     },
    //     3000));
    {
      auto pplan = db->GetPlan("select * from A, B where A.a = B.a;");
      ASSERT_TRUE(pplan->type_ == PlanType::Project);
      ASSERT_TRUE(pplan->ch_->type_ == PlanType::HashJoin);
      auto plan = std::move(pplan->ch_);
      ASSERT_TRUE(plan->ch_ && (plan->ch_->type_ == PlanType::SeqScan || plan->ch_->type_ == PlanType::RangeScan));
      ASSERT_TRUE(plan->ch2_ && (plan->ch2_->type_ == PlanType::SeqScan || plan->ch2_->type_ == PlanType::RangeScan));
      if (plan->ch_->type_ == PlanType::SeqScan) {
        ASSERT_TRUE(static_cast<SeqScanPlanNode*>(plan->ch_.get())->table_name_ == "B");
      } else {
        ASSERT_TRUE(static_cast<RangeScanPlanNode*>(plan->ch_.get())->table_name_ == "B");
      }
      if (plan->ch2_->type_ == PlanType::SeqScan) {
        ASSERT_TRUE(static_cast<SeqScanPlanNode*>(plan->ch2_.get())->table_name_ == "A");
      } else {
        ASSERT_TRUE(static_cast<RangeScanPlanNode*>(plan->ch2_.get())->table_name_ == "A");
      }  
    }
    
    
    // DB_INFO("Use: {} s", sw.GetTimeInSeconds());
    // sw.Reset();


    // You should swap dupA and A. because b is uniformly distributed in [0,
    // 100]. So dupA's size is 1e6 / 10000.

    // EXPECT_TRUE(test_timeout(
    //     [&]() {
    //       // clang-format off
    //       db->Execute("select * from A, A as dupA where A.a = dupA.a and dupA.b = 5;");
    //       // clang-format on
    //     },
    //     3000));
    // DB_INFO("Use: {} s", sw.GetTimeInSeconds());

    {
      auto pplan = db->GetPlan("select * from A, A as dupA where A.a = dupA.a and dupA.b = 5;");
      ASSERT_TRUE(pplan->type_ == PlanType::Project);
      ASSERT_TRUE(pplan->ch_->type_ == PlanType::HashJoin);
      auto plan = std::move(pplan->ch_);
      ASSERT_TRUE(plan->ch_ && (plan->ch_->type_ == PlanType::SeqScan || plan->ch_->type_ == PlanType::RangeScan));
      ASSERT_TRUE(plan->ch2_ && (plan->ch2_->type_ == PlanType::SeqScan || plan->ch2_->type_ == PlanType::RangeScan));
      if (plan->ch_->type_ == PlanType::SeqScan) {
        ASSERT_TRUE(static_cast<SeqScanPlanNode*>(plan->ch_.get())->predicate_.GetVec().size() != 0);
      } else {
        ASSERT_TRUE(static_cast<RangeScanPlanNode*>(plan->ch_.get())->predicate_.GetVec().size() != 0);
      }
      if (plan->ch2_->type_ == PlanType::SeqScan) {
        ASSERT_TRUE(static_cast<SeqScanPlanNode*>(plan->ch2_.get())->predicate_.GetVec().size() == 0);
      } else {
        ASSERT_TRUE(static_cast<RangeScanPlanNode*>(plan->ch2_.get())->predicate_.GetVec().size() == 0);
      }  
    }

    {
      auto pplan = db->GetPlan("select * from A as dupA, A where A.a = dupA.a and dupA.b = 5;");
      ASSERT_TRUE(pplan->type_ == PlanType::Project);
      ASSERT_TRUE(pplan->ch_->type_ == PlanType::HashJoin);
      auto plan = std::move(pplan->ch_);
      ASSERT_TRUE(plan->ch_ && (plan->ch_->type_ == PlanType::SeqScan || plan->ch_->type_ == PlanType::RangeScan));
      ASSERT_TRUE(plan->ch2_ && (plan->ch2_->type_ == PlanType::SeqScan || plan->ch2_->type_ == PlanType::RangeScan));
      if (plan->ch_->type_ == PlanType::SeqScan) {
        ASSERT_TRUE(static_cast<SeqScanPlanNode*>(plan->ch_.get())->predicate_.GetVec().size() != 0);
      } else {
        ASSERT_TRUE(static_cast<RangeScanPlanNode*>(plan->ch_.get())->predicate_.GetVec().size() != 0);
      }
      if (plan->ch2_->type_ == PlanType::SeqScan) {
        ASSERT_TRUE(static_cast<SeqScanPlanNode*>(plan->ch2_.get())->predicate_.GetVec().size() == 0);
      } else {
        ASSERT_TRUE(static_cast<RangeScanPlanNode*>(plan->ch2_.get())->predicate_.GetVec().size() == 0);
      }  
    }
  }

  db = nullptr;
  std::filesystem::remove("__tmp0203");
}

TEST(OptimizerTest, JoinAssociate4Test) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0204");
  auto db = std::make_unique<wing::Instance>("__tmp0204", SAKURA_USE_JIT_FLAG);
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
    RandomTuple<int64_t, std::string> tuple_gen(0x202303091108, 0, 0, 20, 20);
    auto [stmt_e, data_e] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Employee " + stmt_e + ";").Valid());
    auto [stmt_u, data_u] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Username " + stmt_u + ";").Valid());
    auto [stmt_c, data_c] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Catname " + stmt_c + ";").Valid());
    RandomTuple<int64_t, int64_t, int64_t> tuple_gen2(
        0x202303091110, 1, NUM, 1, NUM, 1, NUM);
    auto [stmt_cats, data_cats] = tuple_gen2.GenerateValuesClause(CATNUM);
    EXPECT_TRUE(db->Execute("insert into Cats " + stmt_cats + ";").Valid());

    db->Analyze("Employee");
    db->Analyze("Username");
    db->Analyze("Catname");
    db->Analyze("Cats");

    StopWatch sw;
    EXPECT_TRUE(test_timeout(
        [&]() {
          // clang-format off
          db->Execute("select * from Employee, Username, Catname, Cats where Cats.emp_id = Employee.id and Cats.username_id = Username.id and Cats.cat_id = Catname.id;");
          // clang-format on
        },
        5000));
    DB_INFO("Use: {} s", sw.GetTimeInSeconds());
  }
  db = nullptr;
  std::filesystem::remove("__tmp0204");
}

TEST(OptimizerTest, JoinAssociate5Test) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0205");
  auto db = std::make_unique<wing::Instance>("__tmp0205", SAKURA_USE_JIT_FLAG);
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
    RandomTuple<int64_t, std::string> tuple_gen(0x202303091108, 0, 0, 20, 20);
    auto [stmt_e, data_e] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Employee " + stmt_e + ";").Valid());
    auto [stmt_u, data_u] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Username " + stmt_u + ";").Valid());
    auto [stmt_c, data_c] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Catname " + stmt_c + ";").Valid());
    auto [stmt_m, data_m] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Maidname " + stmt_m + ";").Valid());
    RandomTuple<int64_t, int64_t, int64_t, int64_t> tuple_gen2(
        0x202303091110, 1, NUM, 1, NUM, 1, NUM, 1, NUM);
    auto [stmt_cats, data_cats] = tuple_gen2.GenerateValuesClause(CATNUM);
    EXPECT_TRUE(db->Execute("insert into Cats " + stmt_cats + ";").Valid());

    db->Analyze("Employee");
    db->Analyze("Username");
    db->Analyze("Catname");
    db->Analyze("Cats");
    db->Analyze("Maidname");

    int test_round = 10;

    StopWatch sw;
    EXPECT_TRUE(test_timeout(
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
        6000));
    DB_INFO("Use: {} s", sw.GetTimeInSeconds());
    {
      ResultSet result;

      StopWatch sw;
      EXPECT_TRUE(test_timeout(
        [&]() {
          result = db->Execute("select count(*) from Cats as B, Cats as C, Cats as D, Cats as E,  Cats as A where "
                      "A.cat_id = B.cat_id and A.emp_id = B.emp_id and A.username_id = B.username_id "
                      "and B.cat_id = C.cat_id and B.maid_id = C.maid_id and B.username_id = C.username_id "
                      "and B.maid_id = E.maid_id and B.emp_id = E.emp_id and B.username_id = E.username_id "
                      "and E.cat_id = D.cat_id and D.emp_id = B.emp_id and D.username_id = B.username_id; ");
        },
        2000));
      DB_INFO("Use: {} s", sw.GetTimeInSeconds());
      EXPECT_TRUE(result.Valid());
      auto tuple = result.Next();
      EXPECT_TRUE(tuple);
      EXPECT_EQ(tuple.ReadInt(0), CATNUM);  
    }
    
  }
  db = nullptr;
  std::filesystem::remove("__tmp0205");
}

template<typename PKType, typename T, T(wing::wing_testing::Value::*ReadPKMethod)() const, typename GenRandomKeyFunc, typename RTGenType, typename DBType, typename GenValueClauseFunc>
void TestPKRangeScan(int NUM, int round, int short_range_round, int short_range_len, int long_range_round, 
RTGenType&& tuple_gen, DBType* db, GenRandomKeyFunc&& rgen_func, GenValueClauseFunc&& vc_func) {
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
    EXPECT_TRUE(test_timeout(
        [&]() {
          // Test point lookup.
          for (int i = 0; i < round; i++) {
            // Insert data.
            EXPECT_TRUE(db->Execute("insert into A " + stmt_data[i].first + ";")
                            .Valid());
            // Insert data into mp.
            for (int j = 0; j < NUM / round; j++) {
              auto key = PKType((stmt_data[i].second.Get(j, 0).get()->*ReadPKMethod)());
              auto value = stmt_data[i].second.Get(j, 1)->ReadFloat();
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
            auto result = db->Execute(
                fmt::format("select * from A where a = {};", vc_func(random_key)));
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
        10000));
    DB_INFO("Use: {} s", sw.GetTimeInSeconds());
    sw.Reset();
    EXPECT_TRUE(test_timeout(
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
                fmt::format("select * from A where a >= {} and a < {};", vc_func(L), vc_func(R)));
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
              auto result = db->Execute(
                  fmt::format("select * from A where a {} {};", ops[j], vc_func(L)));
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
        10000));
    DB_INFO("Use: {} s", sw.GetTimeInSeconds());
    sw.Reset();

    // Test empty range.
    EXPECT_TRUE(test_timeout([&]() {
      for (int i = 0; i < 100; i++) {
        PKType x = rgen_func();
        while (mp.count(x))
          x = rgen_func();
        auto result =
            db->Execute(fmt::format("select * from A where a > {} and a < {}; ", vc_func(x), vc_func(x)));
        EXPECT_TRUE(result.Valid());
        EXPECT_FALSE(result.Next());
      }
    }, 3000));
    DB_INFO("Use: {} s", sw.GetTimeInSeconds());
}

TEST(OptimizerTest, IntegerRangeScanTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0205");
  auto db = std::make_unique<wing::Instance>("__tmp0205", SAKURA_USE_JIT_FLAG);
  // Insert key-value data
  // Read the corresponding value of a random key while inserting.
  {
    EXPECT_TRUE(
        db->Execute("create table A(a int64 primary key, b float64);").Valid());
    int NUM = 1e6, round = 2e5, short_range_round = 5e4, short_range_len = 100;
    int long_range_round = 10;
    std::mt19937_64 rgen(0x202304041223);
    TestPKRangeScan<int64_t, int64_t, &Value::ReadInt>(NUM, round, short_range_round, short_range_len, long_range_round, 
    RandomTuple<int64_t, double>(0x202304041203ll, INT64_MIN, INT64_MAX, 0.0, 1.0), db.get(), 
    [&rgen](){return rgen();}, [](int64_t x) { return x; });
  }
  db = nullptr;
  std::filesystem::remove("__tmp0205");
}

TEST(OptimizerTest, StringRangeScanTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0206");
  auto db = std::make_unique<wing::Instance>("__tmp0206", SAKURA_USE_JIT_FLAG);
  {
    EXPECT_TRUE(
        db->Execute("create table A(a varchar(20) primary key, b float64);").Valid());
    int NUM = 2e5, round = 2e4, short_range_round = 1e4, short_range_len = 100;
    int long_range_round = 10;
    std::mt19937_64 rgen(0x20230418033);
    TestPKRangeScan<std::string, std::string_view, &Value::ReadString>(NUM, round, short_range_round, short_range_len, long_range_round, 
    RandomTuple<std::string, double>(0x202304041203ll, 10, 20, 0.0, 1.0), db.get(), 
    [&rgen]() -> std::string {
      std::string ret(rgen() % 20 + 10, 0);
      char char_set[] =
          "abcdefghijklmnopqrstuvwxyzQWERTYUIOPLKJHGFDSANMZXCVB?:{[1234567890-="
          "/.,!@#$%^&*()]}";
      for (auto& c : ret) c = char_set[rgen() % sizeof(char_set)];
      return ret;
    }, [](std::string x) { return "'" + x + "'"; });
  }
  db = nullptr;
  std::filesystem::remove("__tmp0206");
}

TEST(OptimizerTest, FloatRangeScanTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0207");
  auto db = std::make_unique<wing::Instance>("__tmp0207", SAKURA_USE_JIT_FLAG);
  // Insert key-value data
  // Read the corresponding value of a random key while inserting.
  {
    EXPECT_TRUE(
        db->Execute("create table A(a float64 primary key, b float64);").Valid());
    int NUM = 5e5, round = 1e5, short_range_round = 1e4, short_range_len = 100;
    int long_range_round = 10;
    std::mt19937_64 rgen(0x202304041223);
    TestPKRangeScan<double, double, &Value::ReadFloat>(NUM, round, short_range_round, short_range_len, long_range_round, 
    RandomTuple<double, double>(0x202304041203ll, 0.0, 1e4, 0.0, 1.0), db.get(), 
    [&rgen]() -> double {
      std::uniform_real_distribution<> dis(0.0, 1e4);
      return std::floor(dis(rgen) * 1e4) / 1e4;
    }, [](double x) -> std::string { return fmt::format("{:.10f}", x); });
  }
  db = nullptr;
  std::filesystem::remove("__tmp0207");
}
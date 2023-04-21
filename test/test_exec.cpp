#include <gtest/gtest.h>

#include <filesystem>

#include "common/stopwatch.hpp"
#include "instance/instance.hpp"
#include "test.hpp"

TEST(ExecutorJoinTest, JoinTestNum10Table2) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0100");
  auto db = std::make_unique<wing::Instance>("__tmp0100", SAKURA_USE_JIT_FLAG);

  // Do joins on values.
  {
    auto result = db->Execute("select * from (values(2, 3)), (values(4, 5));");
    EXPECT_TRUE(result.Valid());
    auto tuple = result.Next();
    EXPECT_TRUE(bool(tuple));
    EXPECT_TRUE(tuple.ReadInt(0) == 2);
    EXPECT_TRUE(tuple.ReadInt(1) == 3);
    EXPECT_TRUE(tuple.ReadInt(2) == 4);
    EXPECT_TRUE(tuple.ReadInt(3) == 5);
    EXPECT_FALSE(result.Next());
  }

  // Test duplicate tuple join.
  {
    auto result = db->Execute(
        "select * from (values(1), (1), (1)), (values(2), (2), (2));");
    AnsVec answer;
    EXPECT_TRUE(result.Valid());
    for (int i = 0; i < 9; i++)
      answer.emplace_back(MkVec(IV::Create(1), IV::Create(2)));
    CHECK_ALL_SORTED_ANS(answer, result, 2);
  }

  {
    auto result = db->Execute(
        "select * from (values(1), (1), (1)) A(a), (values(2), (2), (2)) B(b) "
        "where A.a = B.b - 1;");
    AnsVec answer;
    EXPECT_TRUE(result.Valid());
    for (int i = 0; i < 9; i++)
      answer.emplace_back(MkVec(IV::Create(1), IV::Create(2)));
    CHECK_ALL_SORTED_ANS(answer, result, 2);
  }

  {
    auto result =
        db->Execute("select * from (values(2, 3)) join (values(4, 5)) on 1;");
    EXPECT_TRUE(result.Valid());
    auto tuple = result.Next();
    EXPECT_TRUE(bool(tuple));
    EXPECT_TRUE(tuple.ReadInt(0) == 2);
    EXPECT_TRUE(tuple.ReadInt(1) == 3);
    EXPECT_TRUE(tuple.ReadInt(2) == 4);
    EXPECT_TRUE(tuple.ReadInt(3) == 5);
    EXPECT_FALSE(result.Next());
  }

  {
    auto result = db->Execute(
        "select * from (values(2, 3)) as A(a, b) join (values(4, 5)) as A(c, "
        "d) on a = c;");
    EXPECT_TRUE(result.Valid());
    EXPECT_FALSE(result.Next());
  }

  // One database records the number of apples people have. Another records the
  // number of bananas people have. Now I want to know how much money people get
  // when they sell their fruit.
  {
    auto result = db->Execute(
        "select A.name, A.age, A.apple * 3 + B.banana * 7 from "
        "(values('alice', 10, 3), ('bob', 8, 4), ('carol', 15, 9)) as A(name, "
        "age, apple), "
        "(values('alice', 10.5), ('carol', 9.5)) as B(name, banana) where "
        "A.name = B.name;");
    EXPECT_TRUE(result.Valid());
    auto tuple = result.Next();
    EXPECT_TRUE(bool(tuple));
    AnsMap<std::string> answer;
    answer.emplace(
        "alice", MkVec(SV::Create("alice"), IV::Create(10), FV::Create(82.5)));
    answer.emplace(
        "carol", MkVec(SV::Create("carol"), IV::Create(15), FV::Create(93.5)));
    EXPECT_TRUE(CheckAns(tuple.ReadString(0), answer, tuple, 3));
    tuple = result.Next();
    EXPECT_TRUE(bool(tuple));
    EXPECT_TRUE(CheckAns(tuple.ReadString(0), answer, tuple, 3));
    EXPECT_FALSE(result.Next());
  }

  // xxxholic
  {
    EXPECT_TRUE(db->Execute("create table Numbers(t varchar(30) primary key, a "
                            "int32, b float64);")
                    .Valid());
    EXPECT_TRUE(
        db->Execute("insert into Numbers values ('blogaholic', 1, 2.3), "
                    " ('bookaholic', 2, 3.4), "
                    " ('alcoholic', 3, 4.5),"
                    " ('milkaholic', 4, 5.9),"
                    " ('foodaholic', 5, 9.7),"
                    " ('workaholic', 6, 10.99),"
                    " ('chocaholic', 7, 9.2),"
                    " ('lifeaholic', 8, 11.9),"
                    " ('laughaholic', 9, 7.0),"
                    " ('spendaholic', 10, 75.0);")
            .Valid());
    {
      auto result = db->Execute(
          "select A.t, A.b from Numbers as A, Numbers as B where A.a * B.a = "
          "0;");
      EXPECT_TRUE(result.Valid());
      EXPECT_FALSE(result.Next());
    }

    {
      auto result = db->Execute(
          "select A.t, A.b from Numbers as A, Numbers as B where A.a * B.a > "
          "100;");
      EXPECT_TRUE(result.Valid());
      EXPECT_FALSE(result.Next());
    }

    {
      auto result = db->Execute(
          "select A.b, A.t from (values(10), (3), (4)) as C(c), Numbers as A "
          "where A.a = C.c;");
      EXPECT_TRUE(result.Valid());
      AnsMap<std::string> answer;
      answer.emplace(
          "spendaholic", MkVec(FV::Create(75.0), SV::Create("spendaholic")));
      answer.emplace(
          "alcoholic", MkVec(FV::Create(4.5), SV::Create("alcoholic")));
      answer.emplace(
          "milkaholic", MkVec(FV::Create(5.9), SV::Create("milkaholic")));
      CHECK_ALL_ANS(answer, result, tuple.ReadString(1), 2);
    }

    {
      auto result = db->Execute(
          "select A.b, A.t from Numbers as A, (values(1), (8), (-1), (19)) as "
          "C(c) where A.a = C.c;");
      AnsMap<std::string> answer;
      answer.emplace(
          "blogaholic", MkVec(FV::Create(2.3), SV::Create("blogaholic")));
      answer.emplace(
          "lifeaholic", MkVec(FV::Create(11.9), SV::Create("lifeaholic")));
      CHECK_ALL_ANS(answer, result, tuple.ReadString(1), 2);
    }

    {
      auto result = db->Execute(
          "select A.t, B.t, A.b * B.b from Numbers as A, Numbers as B where "
          "A.a * B.a < 50 and A.t < 'm' and B.t > 'bookaholic';");

      AnsMap<std::string> answer;
      answer.emplace("blogaholic_milkaholic",
          MkVec(SV::Create("blogaholic"), SV::Create("milkaholic"),
              FV::Create(13.570)));
      answer.emplace("blogaholic_foodaholic",
          MkVec(SV::Create("blogaholic"), SV::Create("foodaholic"),
              FV::Create(22.310)));
      answer.emplace("blogaholic_workaholic",
          MkVec(SV::Create("blogaholic"), SV::Create("workaholic"),
              FV::Create(25.277)));
      answer.emplace("blogaholic_chocaholic",
          MkVec(SV::Create("blogaholic"), SV::Create("chocaholic"),
              FV::Create(21.160)));
      answer.emplace("blogaholic_lifeaholic",
          MkVec(SV::Create("blogaholic"), SV::Create("lifeaholic"),
              FV::Create(27.370)));
      answer.emplace("blogaholic_laughaholic",
          MkVec(SV::Create("blogaholic"), SV::Create("laughaholic"),
              FV::Create(16.100)));
      answer.emplace("blogaholic_spendaholic",
          MkVec(SV::Create("blogaholic"), SV::Create("spendaholic"),
              FV::Create(172.500)));
      answer.emplace("bookaholic_milkaholic",
          MkVec(SV::Create("bookaholic"), SV::Create("milkaholic"),
              FV::Create(20.060)));
      answer.emplace("bookaholic_foodaholic",
          MkVec(SV::Create("bookaholic"), SV::Create("foodaholic"),
              FV::Create(32.980)));
      answer.emplace("bookaholic_workaholic",
          MkVec(SV::Create("bookaholic"), SV::Create("workaholic"),
              FV::Create(37.366)));
      answer.emplace("bookaholic_chocaholic",
          MkVec(SV::Create("bookaholic"), SV::Create("chocaholic"),
              FV::Create(31.280)));
      answer.emplace("bookaholic_lifeaholic",
          MkVec(SV::Create("bookaholic"), SV::Create("lifeaholic"),
              FV::Create(40.460)));
      answer.emplace("bookaholic_laughaholic",
          MkVec(SV::Create("bookaholic"), SV::Create("laughaholic"),
              FV::Create(23.800)));
      answer.emplace("bookaholic_spendaholic",
          MkVec(SV::Create("bookaholic"), SV::Create("spendaholic"),
              FV::Create(255.000)));
      answer.emplace("alcoholic_milkaholic",
          MkVec(SV::Create("alcoholic"), SV::Create("milkaholic"),
              FV::Create(26.550)));
      answer.emplace("alcoholic_foodaholic",
          MkVec(SV::Create("alcoholic"), SV::Create("foodaholic"),
              FV::Create(43.650)));
      answer.emplace("alcoholic_workaholic",
          MkVec(SV::Create("alcoholic"), SV::Create("workaholic"),
              FV::Create(49.455)));
      answer.emplace("alcoholic_chocaholic",
          MkVec(SV::Create("alcoholic"), SV::Create("chocaholic"),
              FV::Create(41.400)));
      answer.emplace("alcoholic_lifeaholic",
          MkVec(SV::Create("alcoholic"), SV::Create("lifeaholic"),
              FV::Create(53.550)));
      answer.emplace("alcoholic_laughaholic",
          MkVec(SV::Create("alcoholic"), SV::Create("laughaholic"),
              FV::Create(31.500)));
      answer.emplace("alcoholic_spendaholic",
          MkVec(SV::Create("alcoholic"), SV::Create("spendaholic"),
              FV::Create(337.500)));
      answer.emplace("foodaholic_milkaholic",
          MkVec(SV::Create("foodaholic"), SV::Create("milkaholic"),
              FV::Create(57.230)));
      answer.emplace("foodaholic_foodaholic",
          MkVec(SV::Create("foodaholic"), SV::Create("foodaholic"),
              FV::Create(94.090)));
      answer.emplace("foodaholic_workaholic",
          MkVec(SV::Create("foodaholic"), SV::Create("workaholic"),
              FV::Create(106.603)));
      answer.emplace("foodaholic_chocaholic",
          MkVec(SV::Create("foodaholic"), SV::Create("chocaholic"),
              FV::Create(89.240)));
      answer.emplace("foodaholic_lifeaholic",
          MkVec(SV::Create("foodaholic"), SV::Create("lifeaholic"),
              FV::Create(115.430)));
      answer.emplace("foodaholic_laughaholic",
          MkVec(SV::Create("foodaholic"), SV::Create("laughaholic"),
              FV::Create(67.900)));
      answer.emplace("chocaholic_milkaholic",
          MkVec(SV::Create("chocaholic"), SV::Create("milkaholic"),
              FV::Create(54.280)));
      answer.emplace("chocaholic_foodaholic",
          MkVec(SV::Create("chocaholic"), SV::Create("foodaholic"),
              FV::Create(89.240)));
      answer.emplace("chocaholic_workaholic",
          MkVec(SV::Create("chocaholic"), SV::Create("workaholic"),
              FV::Create(101.108)));
      answer.emplace("chocaholic_chocaholic",
          MkVec(SV::Create("chocaholic"), SV::Create("chocaholic"),
              FV::Create(84.640)));
      answer.emplace("lifeaholic_milkaholic",
          MkVec(SV::Create("lifeaholic"), SV::Create("milkaholic"),
              FV::Create(70.210)));
      answer.emplace("lifeaholic_foodaholic",
          MkVec(SV::Create("lifeaholic"), SV::Create("foodaholic"),
              FV::Create(115.430)));
      answer.emplace("lifeaholic_workaholic",
          MkVec(SV::Create("lifeaholic"), SV::Create("workaholic"),
              FV::Create(130.781)));
      answer.emplace("laughaholic_milkaholic",
          MkVec(SV::Create("laughaholic"), SV::Create("milkaholic"),
              FV::Create(41.300)));
      answer.emplace("laughaholic_foodaholic",
          MkVec(SV::Create("laughaholic"), SV::Create("foodaholic"),
              FV::Create(67.900)));

      for (uint32_t i = 0; i < answer.size(); i++) {
        auto tuple = result.Next();
        EXPECT_TRUE(bool(tuple));
        EXPECT_TRUE(CheckAns(
            fmt::format("{}_{}", tuple.ReadString(0), tuple.ReadString(1)),
            answer, tuple, 3));
      }
      EXPECT_FALSE(result.Next());
    }
  }

  db = nullptr;
  std::filesystem::remove("__tmp0100");
}

TEST(ExecutorJoinTest, JoinTestNum3e3Table2) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0101");
  auto db = std::make_unique<wing::Instance>("__tmp0101", SAKURA_USE_JIT_FLAG);
  auto NUM = 3e3;
  // Two countries have 3e3 ports. Ports have three attributes: name, position,
  // cost, time. Alice wants to find the shortest path from Country A to Country
  // B within the given cost. But we don't have aggregate operators, so she only
  // lists those paths. Cost: c_0 * ||position_0 - position_1|| ^ 2 + cost_0 +
  // cost_1 Time: ||position_0 - position_1|| ^ 2 / t_0  + time_0 + time_1
  wing_testing::RandomTuple<std::string, double, double, double, double, double>
      tuple_gen(114530, 10, 20, 0.0, 10.0, 0.0, 10.0, 0.0, 10.0, 0.0, 1000.0,
          0.0, 1.0);
  EXPECT_TRUE(db->Execute("create table countryA(name varchar(20), px float64, "
                          "py float64, pz float64, cost float64, t float64);")
                  .Valid());
  EXPECT_TRUE(db->Execute("create table countryB(name varchar(20), px float64, "
                          "py float64, pz float64, cost float64, t float64);")
                  .Valid());
  auto [stmt_a, data_a] = tuple_gen.GenerateValuesClause(NUM);
  EXPECT_TRUE(db->Execute("insert into countryA " + stmt_a + ";").Valid());
  auto [stmt_b, data_b] = tuple_gen.GenerateValuesClause(NUM);
  EXPECT_TRUE(db->Execute("insert into countryB " + stmt_b + ";").Valid());
  double c0 = 20, t0 = 5, lim_cost = 700;
  StopWatch sw;
  auto result = db->Execute(
      fmt::format("select A.name, B.name, "
                  "((A.px-B.px)*(A.px-B.px)+(A.py-B.py)*(A.py-B.py)+(A.pz-B.pz)"
                  "*(A.pz-B.pz)) / {} + A.t + B.t from countryA "
                  "as A, countryB as B where  "
                  "((A.px-B.px)*(A.px-B.px)+(A.py-B.py)*(A.py-B.py)+(A.pz-B.pz)"
                  "*(A.pz-B.pz)) * {} + A.cost + B.cost < {};",
          t0, c0, lim_cost));
  DB_INFO("Use: {} s", sw.GetTimeInSeconds());
  EXPECT_TRUE(result.Valid());
  SortedVec<std::string, PVec> answer;
  for (int i = 0; i < NUM; i++) {
    auto aname = data_a.Get(i, 0)->ReadString();
    double apx = data_a.Get(i, 1)->ReadFloat();
    double apy = data_a.Get(i, 2)->ReadFloat();
    double apz = data_a.Get(i, 3)->ReadFloat();
    double ac = data_a.Get(i, 4)->ReadFloat();
    double at = data_a.Get(i, 5)->ReadFloat();
    for (int j = 0; j < NUM; j++) {
      auto bname = data_b.Get(j, 0)->ReadString();
      double bpx = data_b.Get(j, 1)->ReadFloat();
      double bpy = data_b.Get(j, 2)->ReadFloat();
      double bpz = data_b.Get(j, 3)->ReadFloat();
      double bc = data_b.Get(j, 4)->ReadFloat();
      double bt = data_b.Get(j, 5)->ReadFloat();
      if (((apx - bpx) * (apx - bpx) + (apy - bpy) * (apy - bpy) +
              (apz - bpz) * (apz - bpz)) *
                  c0 +
              ac + bc <
          lim_cost) {
        answer.Append(fmt::format("{}_{}", aname, bname),
            MkVec(SV::Create(aname), SV::Create(bname),
                FV::Create(
                    ((apx - bpx) * (apx - bpx) + (apy - bpy) * (apy - bpy) +
                        (apz - bpz) * (apz - bpz)) /
                        t0 +
                    at + bt)));
      }
    }
  }

  answer.Sort();
  CHECK_ALL_ANS(answer, result,
      fmt::format("{}_{}", tuple.ReadString(0), tuple.ReadString(1)), 3);

  db = nullptr;
  std::filesystem::remove("__tmp0101");
}

TEST(ExecutorJoinTest, JoinTestTable3) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0102");
  auto db = std::make_unique<wing::Instance>("__tmp0102", SAKURA_USE_JIT_FLAG);
  // Two databases have some users and their id, now give the relations between
  // the two user groups, print the corresponding username.
  {
    auto result = db->Execute(
        "select '1', A.name, B.name from (values('alice', 2)) as A(name, id), "
        "(values('bob', 3)) as B(name, id), (values(2, 3), (2, 4), (1, 3)) as "
        "AB(ida, idb) where "
        "A.id = AB.ida and AB.idb = B.id;");
    EXPECT_TRUE(result.Valid());
    AnsMap<std::string> answer;
    answer.emplace(
        "1", MkVec(SV::Create("1"), SV::Create("alice"), SV::Create("bob")));
    CHECK_ALL_ANS(answer, result, tuple.ReadString(0), 3);
  }

  // Shuffle the tables.
  {
    auto result = db->Execute(
        "select '1', A.name, B.name from (values('alice', 2)) as A(name, id),  "
        "(values(2, 3), (2, 4), (1, 3)) as AB(ida, idb), (values('bob', 3)) as "
        "B(name, id) where "
        "A.id = AB.ida and AB.idb = B.id;");
    EXPECT_TRUE(result.Valid());
    AnsMap<std::string> answer;
    answer.emplace(
        "1", MkVec(SV::Create("1"), SV::Create("alice"), SV::Create("bob")));
    CHECK_ALL_ANS(answer, result, tuple.ReadString(0), 3);
  }

  // insert duplicate tuples.
  {
    EXPECT_TRUE(db->Execute("create table dupA(a int64);").Valid());
    EXPECT_TRUE(db->Execute("create table dupB(a int64);").Valid());
    wing_testing::RandomTuple<int64_t> tuple_gen(112930, 0, 1);
    int NUM = 1e5;
    auto [stmt_a, data_a] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into dupA " + stmt_a + ";").Valid());
    auto [stmt_b, data_b] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into dupB " + stmt_b + ";").Valid());
    EXPECT_TRUE(db->Execute("insert into dupA values(100000);").Valid());
    EXPECT_TRUE(db->Execute("insert into dupB values(100000);").Valid());
    ResultSet result;
    StopWatch sw;
    EXPECT_TRUE(test_timeout(
        [&]() {
          result = db->Execute(
              "select * from dupA as A join dupB as B on A.a = B.a + 10;");
        },
        5000));
    DB_INFO("Use: {} s", sw.GetTimeInSeconds());
  }

  // Suppose there are two groups of users.
  // Print the user pairs that have similar values.
  {
    EXPECT_TRUE(db->Execute("create table userA(name varchar(5), id int32 "
                            "auto_increment primary key, value int32);")
                    .Valid());
    EXPECT_TRUE(db->Execute("create table userB(name varchar(5), id int32 "
                            "auto_increment primary key, value int32);")
                    .Valid());
    EXPECT_TRUE(
        db->Execute(
              "create table relation(id int32 auto_increment primary key, ida "
              "int32 foreign key references userA(id), idb int32 "
              "foreign key references userB(id));")
            .Valid());
    int v0 = 20;
    wing_testing::RandomTuple<std::string, int32_t, int32_t> tuple_gen(
        114900, 2, 5, 0, 0, -50, 50);
    int NUM = 3e5, SNUM = NUM * 3;
    auto [stmt_a, data_a] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into userA " + stmt_a + ";").Valid());
    auto [stmt_b, data_b] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into userB " + stmt_b + ";").Valid());
    wing_testing::RandomTuple<int32_t, int32_t, int32_t> tuple_gen_rel(
        114901, 0, 0, 1, NUM, 1, NUM);
    auto [stmt_r, data_r] = tuple_gen_rel.GenerateValuesClause(SNUM);
    EXPECT_TRUE(db->Execute("insert into relation " + stmt_r + ";").Valid());
    ResultSet result;
    StopWatch sw;
    EXPECT_TRUE(test_timeout(
        [&]() {
          result = db->Execute(fmt::format(
              "select R.id, A.name, B.name from userA as A join relation as R "
              "on A.id = R.ida join userB as B on B.id = R.idb where "
              "A.value - B.value < {} and A.value - B.value > {};",
              v0, -v0));
        },
        5000));
    DB_INFO("Use: {} s", sw.GetTimeInSeconds());
    SortedVec<uint32_t, PVec> A;
    SortedVec<uint32_t, PVec> B;
    for (int i = 0; i < NUM; i++) {
      A.Append(i + 1, MkVec(SV::Create(data_a.Get(i, 0)->ReadString()),
                          IV::Create(data_a.Get(i, 2)->ReadInt())));
    }
    for (int i = 0; i < NUM; i++) {
      B.Append(i + 1, MkVec(SV::Create(data_b.Get(i, 0)->ReadString()),
                          IV::Create(data_b.Get(i, 2)->ReadInt())));
    }
    HashAnsMap<uint32_t> answer;
    for (int i = 0; i < SNUM; i++) {
      auto x = data_r.Get(i, 1)->ReadInt();
      auto y = data_r.Get(i, 2)->ReadInt();
      auto ap = A.find(x);
      auto bp = B.find(y);
      if (!ap || !bp)
        continue;
      if (abs((*ap)[1]->ReadInt() - (*bp)[1]->ReadInt()) < v0) {
        answer.emplace(
            i + 1, MkVec(IV::Create(i + 1), SV::Create((*ap)[0]->ReadString()),
                       SV::Create((*bp)[0]->ReadString())));
      }
    }
    DB_INFO("{}", answer.size());
    CHECK_ALL_ANS(answer, result, tuple.ReadInt(0), 3);
  }

  db = nullptr;
  std::filesystem::remove("__tmp0102");
}

TEST(ExecutorJoinTest, JoinTestTableN) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0106");
  auto db = std::make_unique<wing::Instance>("__tmp0106", SAKURA_USE_JIT_FLAG);
  {
    auto result = db->Execute(
        "select * from (values('2', '3'), ('x', 'y')) A(a, b), (values('3', "
        "'4'), ('x', 'y')) B(a, b), (values('4', '5'), ('x', 'y')) C(a, b), "
        "(values('5', '6'), ('x', 'y')) D(a, b), (values('6', '7'), ('x', "
        "'y')) E(a, b), (values('7', '8'), ('x', 'y')) F(a, b), (values('8', "
        "'9'), "
        "('x', 'y')) G(a, b) where A.b = B.a and B.b = C.a and C.b = D.a and "
        "D.b = E.a and E.b = F.a and F.b = G.a;");
    EXPECT_TRUE(result.Valid());
    AnsMap<uint32_t> answer;
    answer.emplace(0,
        MkVec(SV::Create("2"), SV::Create("3"), SV::Create("3"),
            SV::Create("4"), SV::Create("4"), SV::Create("5"), SV::Create("5"),
            SV::Create("6"), SV::Create("6"), SV::Create("7"), SV::Create("7"),
            SV::Create("8"), SV::Create("8"), SV::Create("9")));
    CHECK_ALL_ANS(answer, result, 0, 1);
  }

  // Consider there are some tables A, B, C, D, E containing people.
  // And there are some relations between AB, BC, CD, DE.
  // AB: B follows A;
  // BC: C follows B;
  // CD: D is an employee in C's company;
  // DE: D and E are in the one group. (group is calculated by group_num / 5 ^
  // group_num) We want to query 5 people that satisfy the relations.
  {
    // clang-format off
    EXPECT_TRUE(db->Execute("create table A(name varchar(5), id int32 auto_increment primary key, magic_string varchar(20));").Valid());
    EXPECT_TRUE(db->Execute("create table B(name varchar(5), id int32 auto_increment primary key, magic_string varchar(20));").Valid());
    EXPECT_TRUE(db->Execute("create table C(name varchar(5), id int32 auto_increment primary key, magic_string varchar(20));").Valid());
    EXPECT_TRUE(db->Execute("create table D(name varchar(5), id int32 auto_increment primary key, magic_string varchar(20), group_num int32);").Valid());
    EXPECT_TRUE(db->Execute("create table E(name varchar(5), id int32 auto_increment primary key, magic_string varchar(20), group_num int32);").Valid());
    EXPECT_TRUE(db->Execute("create table FollowsAB(id int32 auto_increment primary key, ida int32 foreign key references A(id), idb int32 foreign key references B(id));").Valid());
    EXPECT_TRUE(db->Execute("create table FollowsBC(id int32 auto_increment primary key, idb int32 foreign key references B(id), idc int32 foreign key references C(id));").Valid());
    EXPECT_TRUE(db->Execute("create table Company(id int32 auto_increment primary key, name varchar(20));").Valid());
    EXPECT_TRUE(db->Execute("create table CompanyC(id int32 auto_increment primary key, c_id int32 foreign key references C(id), company_id int32 foreign key references Company(id));").Valid());
    EXPECT_TRUE(db->Execute("create table Employees(id int32 auto_increment primary key, company_id int32 foreign key references Company(id), d_id int32 foreign key references D(id));").Valid());
    // clang-format on
    int NUM = 100;    // A, B, C size
    int DNUM = 1e3;   // D, E size
    int FNUM = 6000;  // FollowsAB, FollowsBC size
    int CNUM = 80;    // Company number.
    int CONUM = 200;  // Company owner number.
    RandomTuple<std::string, int32_t, std::string> tuple_gen(
        118900, 5, 5, 0, 0, 15, 20);
    auto [stmt_a, data_a] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into A " + stmt_a + ";").Valid());
    auto [stmt_b, data_b] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into B " + stmt_b + ";").Valid());
    auto [stmt_c, data_c] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into C " + stmt_c + ";").Valid());

    RandomTuple<std::string, int32_t, std::string, int32_t> tuple_gen2(
        118901, 5, 5, 0, 0, 15, 20, 0, 100000);
    auto [stmt_d, data_d] = tuple_gen2.GenerateValuesClause(DNUM);
    EXPECT_TRUE(db->Execute("insert into D " + stmt_d + ";").Valid());
    auto [stmt_e, data_e] = tuple_gen2.GenerateValuesClause(DNUM);
    EXPECT_TRUE(db->Execute("insert into E " + stmt_e + ";").Valid());

    RandomTuple<int32_t, std::string> tuple_com(119302, 0, 0, 15, 20);
    auto [stmt_com, data_com] = tuple_com.GenerateValuesClause(CNUM);
    EXPECT_TRUE(db->Execute("insert into Company " + stmt_com + ";").Valid());

    RandomTuple<int32_t, int32_t, int32_t> tuple_com_c(
        119456, 0, 0, 1, NUM, 1, CNUM);
    auto [stmt_comown, data_comown] = tuple_com_c.GenerateValuesClause(CONUM);
    EXPECT_TRUE(
        db->Execute("insert into CompanyC " + stmt_comown + ";").Valid());

    RandomTuple<int32_t, int32_t, int32_t> tuple_emp(
        119888, 0, 0, 1, CNUM, 1, DNUM);
    auto [stmt_emp, data_emp] = tuple_emp.GenerateValuesClause(DNUM);
    EXPECT_TRUE(db->Execute("insert into Employees " + stmt_emp + ";").Valid());

    std::vector<std::pair<int, int>> R;
    for (int i = 1; i <= NUM; i++)
      for (int j = 1; j <= NUM; j++)
        R.emplace_back(i, j);
    std::shuffle(R.begin(), R.end(), std::mt19937_64(119000));
    auto rel_AB = std::vector<std::pair<int, int>>(R.begin(), R.begin() + FNUM);
    {
      std::string stmt = "insert into FollowsAB values ";
      for (int i = 0; i < FNUM; i++) {
        stmt += fmt::format("(0, {}, {}),", R[i].first, R[i].second);
      }
      stmt.back() = ';';
      EXPECT_TRUE(db->Execute(stmt).Valid());
    }
    std::shuffle(R.begin(), R.end(), std::mt19937_64(119001));
    auto rel_BC = std::vector<std::pair<int, int>>(R.begin(), R.begin() + FNUM);
    {
      std::string stmt = "insert into FollowsBC values ";
      for (int i = 0; i < FNUM; i++) {
        stmt += fmt::format("(0, {}, {}),", R[i].first, R[i].second);
      }
      stmt.back() = ';';
      EXPECT_TRUE(db->Execute(stmt).Valid());
    }
    // clang-format off
    auto result = db->Execute(
        "select * from (((A join FollowsAB on FollowsAB.ida = A.id join B on FollowsAB.idb = B.id) join FollowsBC on B.id = FollowsBC.idb join C on C.id = FollowsBC.idc) join (Company join CompanyC on Company.id = CompanyC.company_id) on CompanyC.c_id = C.id) join ((D join E on (D.group_num / 5 ^ D.group_num) = (E.group_num / 5 ^ E.group_num)) join Employees on Employees.d_id = D.id) on Employees.company_id = Company.id;");
    // clang-format on
    EXPECT_TRUE(result.Valid());
    // Calculate DE.
    std::unordered_map<int, std::vector<std::pair<int, int>>> DE;
    for (int d = 0; d < DNUM; d++) {
      auto dx = data_d.Get(d, 3)->ReadInt();
      for (int e = 0; e < DNUM; e++) {
        auto ex = data_e.Get(e, 3)->ReadInt();
        if ((dx / 5 ^ dx) == (ex / 5 ^ ex)) {
          DE[d].emplace_back(d, e);
        }
      }
    }

    // Calculate CDE.
    std::unordered_map<int, std::vector<std::pair<int, int>>> CDE;
    int cdesz = 0;
    for (int c = 0; c < NUM; ++c) {
      for (int co = 0; co < CONUM; co++)
        if (c + 1 == data_comown.Get(co, 1)->ReadInt()) {
          auto& vec = CDE[c];
          auto company_id = data_comown.Get(co, 2)->ReadInt();
          for (int d = 0; d < DNUM; d++)
            if (data_emp.Get(d, 1)->ReadInt() == company_id) {
              auto& dvec = DE[data_emp.Get(d, 2)->ReadInt() - 1];
              cdesz += dvec.size();
              // cdesz += 1;
              vec.insert(vec.end(), dvec.begin(), dvec.end());
            }
        }
    }

    int sz = 0;
    // Calculate ABC
    for (auto [b0, c] : rel_BC) {
      auto& cvec = CDE[c - 1];
      for (auto [a, b] : rel_AB)
        if (b == b0) {
          sz += cvec.size();
        }
    }

    int ssz = 0;
    while (result.Next())
      ssz++;
    EXPECT_EQ(sz, ssz);
  }
  db = nullptr;
  std::filesystem::remove("__tmp0106");
}

TEST(ExecutorAggregateTest, SmallAggregateTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0103");
  auto db = std::make_unique<wing::Instance>("__tmp0103", SAKURA_USE_JIT_FLAG);
  {
    auto result = db->Execute(
        "select sum(a), max(a), count(a), count(*), count(1), min(a), avg(a) "
        "from (values(114514),(1919810),(111222333)) as A(a);");
    AnsMap<uint32_t> answer;
    answer.emplace(3, MkVec(IV::Create(113256657), IV::Create(111222333),
                          IV::Create(3), IV::Create(3), IV::Create(3),
                          IV::Create(114514), FV::Create(37752219)));
    CHECK_ALL_ANS(answer, result, tuple.ReadInt(2), 7);
  }
  {
    auto result = db->Execute(
        "select sum(a) + sum(b), avg(a) * count(a) + avg(b) * count(b) from "
        "(values(2, 3), (4, 5), (6, 7), (8, 9)) A(a, b);");
    AnsMap<uint32_t> answer;
    answer.emplace(0, MkVec(IV::Create(44), FV::Create(44)));
    CHECK_ALL_ANS(answer, result, 0, 2);
  }
  // Now, we don't support NULL.
  // {
  //   // Should return null, but this is not implemented now. So we only test
  //   the number of result tuples. auto result = db->Execute("select sum(a)
  //   from (select 1 where 0) A(a);"); EXPECT_TRUE(result.Valid());
  //   EXPECT_TRUE(bool(result.Next()));
  //   EXPECT_FALSE(result.Next());
  // }
  // {
  //   // Should return null, but this is not implemented now. So we only test
  //   the number of result tuples. auto result = db->Execute("select * from
  //   (select sum(a) from (select 1 where 0) A(a));");
  //   EXPECT_TRUE(result.Valid());
  //   EXPECT_TRUE(bool(result.Next()));
  //   EXPECT_FALSE(result.Next());
  // }
  // {
  //   auto result = db->Execute("select count(*) from (select 1 where 1 < 0)
  //   A(a);"); EXPECT_TRUE(result.Valid()); AnsMap<uint32_t> answer;
  //   answer.emplace(0, MkVec(IV::Create(0)));
  //   CHECK_ALL_ANS(answer, result, 0, 1);
  // }
  {
    auto result =
        db->Execute("select sum(a) from (select 1 where 1) A(a) group by a;");
    EXPECT_TRUE(result.Valid());
    AnsMap<uint32_t> answer;
    answer.emplace(0, MkVec(IV::Create(1)));
    CHECK_ALL_ANS(answer, result, 0, 1);
  }
  {
    auto result = db->Execute(
        "select sum(b) from (select 1 + a * a from (values(2), (3), (4)) V(a) "
        "where 1) A(b);");
    EXPECT_TRUE(result.Valid());
    AnsMap<uint32_t> answer;
    answer.emplace(0, MkVec(IV::Create(32)));
    CHECK_ALL_ANS(answer, result, 0, 1);
  }
  {
    auto result = db->Execute(
        "select a % 2, sum(b) from (select a, 1 + a * a from (values(2), (3), "
        "(4)) V(a) where 1) A(a, b) group by a % 2;");
    EXPECT_TRUE(result.Valid());
    AnsMap<uint32_t> answer;
    answer.emplace(0, MkVec(IV::Create(0), IV::Create(22)));
    answer.emplace(1, MkVec(IV::Create(1), IV::Create(10)));
    CHECK_ALL_ANS(answer, result, tuple.ReadInt(0), 2);
  }
  {
    auto result = db->Execute(
        "select a % 2, sum(b) from (select a, 1 + a * a from (values(2), (3), "
        "(4)) V(a) where 1) A(a, b) group by a % 2 having sum(b) < 22;");
    EXPECT_TRUE(result.Valid());
    AnsMap<uint32_t> answer;
    answer.emplace(0, MkVec(IV::Create(1), IV::Create(10)));
    CHECK_ALL_ANS(answer, result, 0, 2);
  }
  {
    auto result = db->Execute(
        "select a, b from (values('a', 'b'), ('a', 'c'), ('b', 'a')) A(a, b) "
        "group by b, a;");
    EXPECT_TRUE(result.Valid());
    AnsMap<std::string> answer;
    answer.emplace("a_b", PVec());
    answer.emplace("b_a", PVec());
    answer.emplace("a_c", PVec());
    CHECK_ALL_ANS(answer, result,
        fmt::format("{}_{}", tuple.ReadString(0), tuple.ReadString(1)), 0);
  }
  {
    auto result = db->Execute(
        "select a <= 'a' from (values('a', 'b'), ('a', 'c'), ('b', 'a')) A(a, "
        "b) group by a <= 'a', 'm';");
    EXPECT_TRUE(result.Valid());
    AnsMap<uint32_t> answer;
    answer.emplace(0, PVec());
    answer.emplace(1, PVec());
    CHECK_ALL_ANS(answer, result, tuple.ReadInt(0), 0);
  }
  {
    auto result = db->Execute(
        "select a <= 'a' from (values('a', 'b'), ('a', 'c'), ('b', 'a')) A(a, "
        "b) group by 1, a <= 'a', 0;");
    EXPECT_TRUE(result.Valid());
    AnsMap<uint32_t> answer;
    answer.emplace(0, PVec());
    answer.emplace(1, PVec());
    CHECK_ALL_ANS(answer, result, tuple.ReadInt(0), 0);
  }
  {
    auto result = db->Execute(
        "select a < 'a' from (values('a', 'b'), ('a', 'c'), ('b', 'a')) A(a, "
        "b) group by a <= 'a', 'm';");
    EXPECT_TRUE(result.Valid());
    AnsVec answer;
    answer.emplace_back(MkVec(IV::Create(0)));
    answer.emplace_back(MkVec(IV::Create(0)));
    CHECK_ALL_SORTED_ANS(answer, result, 1);
  }
  {
    EXPECT_TRUE(
        db->Execute("create table FlightRecord(date varchar(20), height "
                    "float64, score float64, card varchar(2), user_id int32);")
            .Valid());
    EXPECT_TRUE(db->Execute("insert into FlightRecord values(\'2022-3-1\', "
                            "1.0, 10.0, \'x\', 1);")
                    .Valid());
    EXPECT_TRUE(db->Execute("insert into FlightRecord values(\'2022-3-2\', "
                            "10.0, 30.0, \'w\', 1);")
                    .Valid());
    EXPECT_TRUE(db->Execute("insert into FlightRecord values(\'2022-3-4\', "
                            "101.0, 40.0, \'f\', 1);")
                    .Valid());
    EXPECT_TRUE(db->Execute("insert into FlightRecord values(\'2022-3-3\', "
                            "4.0, 9.0, \'w\', 1);")
                    .Valid());
    EXPECT_TRUE(db->Execute("insert into FlightRecord values(\'2022-3-5\', "
                            "76.0, 59.0, \'f\', 1);")
                    .Valid());
    EXPECT_TRUE(db->Execute("insert into FlightRecord values(\'2022-3-3\', "
                            "100.0, 70.0, \'w\', 1);")
                    .Valid());
    EXPECT_TRUE(db->Execute("insert into FlightRecord values(\'2022-3-5\', "
                            "32.0, 59.0, \'p\', 1);")
                    .Valid());
    EXPECT_TRUE(db->Execute("insert into FlightRecord values(\'2022-3-4\', "
                            "85.0, 90.0, \'p\', 1);")
                    .Valid());
    EXPECT_TRUE(db->Execute("insert into FlightRecord values(\'2022-3-5\', "
                            "12.0, 50.0, \'p\', 1);")
                    .Valid());
    EXPECT_TRUE(db->Execute("insert into FlightRecord values(\'2022-3-10\', "
                            "90.0, 61.0, \'x\', 1);")
                    .Valid());
    {
      auto result =
          db->Execute("select avg(score) from FlightRecord where height > 50;");
      AnsMap<uint32_t> answer;
      answer.emplace(0, MkVec(FV::Create(320.0 / 5)));
      CHECK_ALL_ANS(answer, result, 0, 1);
    }
    {
      auto result = db->Execute(
          "select date, avg(score) from FlightRecord group by date having "
          "avg(score) > 60;");
      AnsMap<std::string> answer;
      answer.emplace(
          "2022-3-10", MkVec(SV::Create("2022-3-10"), FV::Create(61)));
      answer.emplace("2022-3-4", MkVec(SV::Create("2022-3-4"), FV::Create(65)));
      CHECK_ALL_ANS(answer, result, tuple.ReadString(0), 2);
    }
    {
      auto result = db->Execute(
          "select date, max(score) from FlightRecord where card = 'p' group by "
          "date;");
      AnsMap<std::string> answer;
      answer.emplace("2022-3-5", MkVec(SV::Create("2022-3-5"), FV::Create(59)));
      answer.emplace("2022-3-4", MkVec(SV::Create("2022-3-4"), FV::Create(90)));
      CHECK_ALL_ANS(answer, result, tuple.ReadString(0), 2);
    }
    {
      auto result = db->Execute(
          "select * from (select avg(score), date, card from FlightRecord "
          "group by date, card) where card = 'p';");
      AnsMap<std::string> answer;
      answer.emplace("2022-3-5",
          MkVec(FV::Create(109 / 2.), SV::Create("2022-3-5"), SV::Create("p")));
      answer.emplace("2022-3-4",
          MkVec(FV::Create(90), SV::Create("2022-3-4"), SV::Create("p")));
      CHECK_ALL_ANS(answer, result, tuple.ReadString(1), 3);
    }
  }
  db = nullptr;
  std::filesystem::remove("__tmp0103");
}

TEST(ExecutorAggregateTest, PolyAggregateTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0104");
  auto db = std::make_unique<wing::Instance>("__tmp0104", SAKURA_USE_JIT_FLAG);
  // Now we can use our executors to calculate polynomial multiplication!
  // We want to calculate A * B * B, where A is of degree 3000, B is of degree
  // 4000.
  {
    EXPECT_TRUE(
        db->Execute("create table PolyA(id int64 primary key, a0 int64);")
            .Valid());
    EXPECT_TRUE(
        db->Execute("create table PolyB(id int64 primary key, a0 int64);")
            .Valid());
    int L = 3000, R = 4000;
    int* A = new int[L];
    int* B = new int[R];
    std::mt19937_64 gen(114600);
    std::uniform_int_distribution<> dis(-1000, 1000);
    for (int i = 0; i < L; i++)
      A[i] = dis(gen);
    for (int i = 0; i < R; i++)
      B[i] = dis(gen);
    {
      std::string stmt = "insert into PolyA values ";
      for (int i = 0; i < L; i++) {
        stmt += fmt::format("({}, {})", i, A[i]);
        if (i != L - 1)
          stmt += ",";
      }
      stmt += ";";
      EXPECT_TRUE(db->Execute(stmt).Valid());
    }
    {
      std::string stmt = "insert into PolyB values ";
      for (int i = 0; i < R; i++) {
        stmt += fmt::format("({}, {})", i, B[i]);
        if (i != R - 1)
          stmt += ",";
      }
      stmt += ";";
      EXPECT_TRUE(db->Execute(stmt).Valid());
    }
    // Initially 0.
    int64_t* C = new int64_t[L + R]{0};
    int64_t* D = new int64_t[L + R * 2]{0};
    for (int i = 0; i < L; i++)
      for (int j = 0; j < R; j++) {
        C[i + j] += int64_t(A[i]) * B[j];
      }
    for (int i = 0; i < L + R - 1; i++)
      for (int j = 0; j < R; j++) {
        D[i + j] += int64_t(C[i]) * B[j];
      }
    {
      ResultSet result;
      StopWatch sw;
      EXPECT_TRUE(test_timeout(
          [&]() {
            result = db->Execute(
                "select id2, sum(val2) from (select C.id + B.id, C.val * B.a0 "
                "from PolyB as B, (select id, sum(val) from (select A.id + "
                "B.id, A.a0 * "
                "B.a0 from PolyA as A, PolyB as B) _(id, val) group by id) "
                "C(id, val)) _(id2, val2) group by id2;");
          },
          10000));
      DB_INFO("Use: {} s", sw.GetTimeInSeconds());

      EXPECT_TRUE(result.Valid());
      DB_INFO("{}", result.GetErrorMsg());
      for (int i = 0; i < L + R * 2 - 2; i++) {
        auto tuple = result.Next();
        EXPECT_TRUE(bool(tuple));
        EXPECT_TRUE(tuple.ReadInt(0) >= 0 && tuple.ReadInt(0) < L + R * 2 - 2);
        EXPECT_EQ(tuple.ReadInt(1), D[tuple.ReadInt(0)]);
      }
      EXPECT_FALSE(result.Next());
    }
    delete[] A;
    delete[] B;
    delete[] C;
    delete[] D;
  }
  db = nullptr;
  std::filesystem::remove("__tmp0104");
}

TEST(ExecutorAggregateTest, StringAggregateTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0105");
  auto db = std::make_unique<wing::Instance>("__tmp0105", SAKURA_USE_JIT_FLAG);
  {
    EXPECT_TRUE(
        db->Execute("create table strA(name varchar(2), value float64);")
            .Valid());
    EXPECT_TRUE(
        db->Execute("create table strB(name varchar(2), value float64);")
            .Valid());
    RandomTuple<std::string, double> tuple_gen(1919810, 2, 2, 0.0, 1.0);
    int NUM = 2e5;
    auto [stmt_a, data_a] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into strA " + stmt_a + ";").Valid());
    auto [stmt_b, data_b] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into strB " + stmt_b + ";").Valid());

    ResultSet result;
    StopWatch sw;
    EXPECT_TRUE(test_timeout(
        [&]() {
          result = db->Execute(
              "select name, sum(value), max(value) / (min(value) + 1.) from "
              "(select strA.name, strA.value * strB.value + 0.75 as value from "
              "strA, "
              "strB where strA.name = strB.name) group by strA.name having "
              "sum(value) < 6e3 and sum(value) > 4e3;");
        },
        5000));
    DB_INFO("Use: {} s", sw.GetTimeInSeconds());
    EXPECT_TRUE(result.Valid());
    std::unordered_map<std::string, std::vector<double>> A, B;
    for (int i = 0; i < NUM; i++) {
      A[std::string(data_a.Get(i, 0)->ReadString())].push_back(
          data_a.Get(i, 1)->ReadFloat());
      B[std::string(data_b.Get(i, 0)->ReadString())].push_back(
          data_b.Get(i, 1)->ReadFloat());
    }
    HashAnsMap<std::string> answer;
    for (auto& [key, va] : A) {
      auto& vb = B[key];
      double vsum = 0, vmx = -1e20, vmn = 1e20;
      for (auto a : va)
        for (auto b : vb) {
          auto vc = a * b + 0.75;
          vsum += vc;
          vmx = std::max(vmx, vc);
          vmn = std::min(vmn, vc);
        }
      if (vsum > 4e3 && vsum < 6e3) {
        answer.emplace(key, MkVec(SV::Create(key), FV::Create(vsum),
                                FV::Create(vmx / (vmn + 1))));
      }
    }
    DB_INFO("{}", answer.size());
    CHECK_ALL_ANS(answer, result, std::string(tuple.ReadString(0)), 3);
  }
  db = nullptr;
  std::filesystem::remove("__tmp0105");
}

TEST(ExecutorOrderByTest, SmallTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0107");
  auto db = std::make_unique<wing::Instance>("__tmp0107", SAKURA_USE_JIT_FLAG);
  {
    // clang-format off
    auto result = db->Execute("select a, b, c from (values(1), (2), (3)) _(a), (values(1), (2), (3)) _(b), (values(1), (2), (3)) _(c) order by a asc, b desc, c asc;");
    // clang-format on
    AnsVec answer;
    for (int i = 1; i <= 3; i++)
      for (int j = 3; j >= 1; --j)
        for (int k = 1; k <= 3; k++)
          answer.emplace_back(
              MkVec(IV::Create(i), IV::Create(j), IV::Create(k)));
    CHECK_ALL_SORTED_ANS(answer, result, 3);
  }
  {
    // clang-format off
    auto result = db->Execute("select a, b, c from (values(1), (2), (3)) _(a), (values('a'), ('b'), ('c'), ('d')) _(b), (values(1), (2), (3)) _(c) order by a asc, c desc, b asc;");
    // clang-format on
    AnsVec answer;
    for (int i = 1; i <= 3; i++)
      for (int k = 3; k >= 1; k--)
        for (char j = 'a'; j <= 'd'; ++j)
          answer.emplace_back(MkVec(
              IV::Create(i), SV::Create(std::string(&j, 1)), IV::Create(k)));
    CHECK_ALL_SORTED_ANS(answer, result, 3);
  }
  // empty query
  {
    auto result = db->Execute(
        "select * from (select 1 where 1 < 0) _(a) order by a asc;");
    EXPECT_TRUE(result.Valid());
    EXPECT_FALSE(result.Next());
  }
  db = nullptr;
  std::filesystem::remove("__tmp0107");
}

TEST(ExecutorOrderByTest, BigTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0113");
  auto db = std::make_unique<wing::Instance>("__tmp0113", SAKURA_USE_JIT_FLAG);
  // A student called Alice wants to do her C++ homework, quicksort.
  // She needs a magical mirror that reflects the correct answer.
  // You, wing, represents the freedom, i.e. possibility, i.e. to-be, should
  // have the ability. String, double, integer 1e6 sorting.
  {
    EXPECT_TRUE(db->Execute("create table Mirror(name varchar(20), value "
                            "float64, age int64);")
                    .Valid());
    int NUM = 1e6;
    RandomTuple<std::string, double, int64_t> tuple_gen(
        202303040114ull, 0, 20, -100.0, 100.0, INT64_MIN, INT64_MAX);
    auto [stmt_m, data_m] = tuple_gen.GenerateValuesClause(NUM);
    EXPECT_TRUE(db->Execute("insert into Mirror " + stmt_m + ";").Valid());

    AnsVec answer;
    for (int i = 0; i < NUM; i++) {
      answer.emplace_back(MkVec(std::move(data_m.Get(i, 0)),
          std::move(data_m.Get(i, 1)), std::move(data_m.Get(i, 2))));
    }

    std::sort(answer.begin(), answer.end(), [](auto& x, auto& y) {
      if (x[0]->ReadString() != y[0]->ReadString()) {
        return x[0]->ReadString() < y[0]->ReadString();
      }
      if (x[2]->ReadInt() != y[2]->ReadInt())
        return x[2]->ReadInt() > y[2]->ReadInt();
      return x[1]->ReadFloat() < y[1]->ReadFloat();
    });

    {
      StopWatch sw;
      ResultSet result;
      EXPECT_TRUE(test_timeout(
          [&]() {
            result = db->Execute(
                "select * from Mirror order by name asc, age desc, value asc;");
          },
          3000));
      CHECK_ALL_SORTED_ANS(answer, result, 3);
      DB_INFO("Use: {} s", sw.GetTimeInSeconds());
    }

    std::sort(answer.begin(), answer.end(), [](auto& x, auto& y) {
      if (x[2]->ReadInt() % 1919 != y[2]->ReadInt() % 1919)
        return x[2]->ReadInt() % 1919 < y[2]->ReadInt() % 1919;
      if (x[0]->ReadString() != y[0]->ReadString()) {
        return x[0]->ReadString() > y[0]->ReadString();
      }
      return x[1]->ReadFloat() < y[1]->ReadFloat();
    });

    // Note that age is signed.
    // i.e., -1 % 1919 = -1.
    {
      StopWatch sw;
      ResultSet result;
      EXPECT_TRUE(test_timeout(
          [&]() {
            result = db->Execute(
                "select * from Mirror order by age % 1919 asc, name desc, "
                "value asc;");
          },
          3000));
      CHECK_ALL_SORTED_ANS(answer, result, 3);
      DB_INFO("Use: {} s", sw.GetTimeInSeconds());
    }

    std::sort(answer.begin(), answer.end(), [](auto& x, auto& y) {
      if (x[1]->ReadFloat() * x[1]->ReadFloat() !=
          y[1]->ReadFloat() * y[1]->ReadFloat())
        return x[1]->ReadFloat() * x[1]->ReadFloat() <
               y[1]->ReadFloat() * y[1]->ReadFloat();
      if (x[2]->ReadInt() != y[2]->ReadInt())
        return x[2]->ReadInt() > y[2]->ReadInt();
      return x[0]->ReadString() < y[0]->ReadString();
    });

    {
      StopWatch sw;
      ResultSet result;
      EXPECT_TRUE(test_timeout(
          [&]() {
            result = db->Execute(
                "select * from Mirror order by value * value asc, age desc, "
                "name asc;");
          },
          3000));
      CHECK_ALL_SORTED_ANS(answer, result, 3);
      DB_INFO("Use: {} s", sw.GetTimeInSeconds());
    }

    std::sort(answer.begin(), answer.end(), [](auto& x, auto& y) {
      if (x[2]->ReadInt() % 2 != y[2]->ReadInt() % 2)
        return x[2]->ReadInt() % 2 < y[2]->ReadInt() % 2;
      if (x[2]->ReadInt() % 5 != y[2]->ReadInt() % 5)
        return x[2]->ReadInt() % 5 < y[2]->ReadInt() % 5;
      if ((x[2]->ReadInt() ^ 114) % 9 != (y[2]->ReadInt() ^ 114) % 9)
        return (x[2]->ReadInt() ^ 114) % 9 < (y[2]->ReadInt() ^ 114) % 9;
      if (x[0]->ReadString() != y[0]->ReadString()) {
        return x[0]->ReadString() > y[0]->ReadString();
      }
      return x[1]->ReadFloat() < y[1]->ReadFloat();
    });

    {
      StopWatch sw;
      ResultSet result;
      EXPECT_TRUE(test_timeout(
          [&]() {
            result = db->Execute(
                "select * from Mirror order by age % 2 asc, age % 5 asc, (age "
                "^ 114) % 9 asc, name desc, value asc;");
          },
          3000));
      CHECK_ALL_SORTED_ANS(answer, result, 3);
      DB_INFO("Use: {} s", sw.GetTimeInSeconds());
    }
  }
  db = nullptr;
  std::filesystem::remove("__tmp0113");
}

TEST(ExecutorLimitTest, SmallTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0108");
  auto db = std::make_unique<wing::Instance>("__tmp0108", SAKURA_USE_JIT_FLAG);
  // empty query
  {
    auto result = db->Execute(
        "select * from (select 1 where 1 < 0) _(a) order by a asc limit 10;");
    EXPECT_TRUE(result.Valid());
    EXPECT_FALSE(result.Next());
  }

  // empty return set
  {
    // clang-format off
    auto result = db->Execute("select a, b, c from (values(1), (2), (3)) _(a), (values(1), (2), (3)) _(b), (values(1), (2), (3)) _(c) order by a asc, b desc, c asc limit 10 offset 30;");
    // clang-format on
    EXPECT_TRUE(result.Valid());
    EXPECT_FALSE(result.Next());
  }

  {
    // clang-format off
    auto result = db->Execute("select * from (values(9), (7), (5), (6), (2), (1), (4), (3), (8)) _(a) order by a asc limit 3 offset 1;");
    // clang-format on
    EXPECT_TRUE(result.Valid());
    AnsVec answer;
    for (int i = 2; i <= 4; i++)
      answer.emplace_back(MkVec(IV::Create(i)));
    CHECK_ALL_SORTED_ANS(answer, result, 1);
  }
  db = nullptr;
  std::filesystem::remove("__tmp0108");
}

TEST(ExecutorLimitTest, BigTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0114");
  auto db = std::make_unique<wing::Instance>("__tmp0114", SAKURA_USE_JIT_FLAG);
  // Solve the equation -x^3 + 17x + 1 = 0.
  {
    EXPECT_TRUE(db->Execute("create table A(x float64);").Valid());
    EXPECT_TRUE(
        db->Execute(
              "insert into A values (5.0), (4.0), (3.0), (2.0), (1.0), (0.0);")
            .Valid());
    EXPECT_TRUE(
        db->Execute(
              "insert into A values (-5.0), (-4.0), (-3.0), (-2.0), (-1.0);")
            .Valid());
    for (int i = 1; i <= 16; i++) {
      EXPECT_TRUE(db->Execute(fmt::format("insert into A select x + {} from A;",
                                  pow(0.5, i)))
                      .Valid());
    }
    // clang-format off
    auto result = db->Execute("select * from (select x as x0 from A where -x * x * x + 17 * x + 1 >= 0 order by -x * x * x + 17 * x + 1 asc limit 3) order by x0 asc;");
    // clang-format on
    EXPECT_TRUE(result.Valid());
    EXPECT_TRUE(fabs(result.Next().ReadFloat(0) - -4.0934) < 0.0001);
    EXPECT_TRUE(fabs(result.Next().ReadFloat(0) - -0.0588) < 0.0001);
    EXPECT_TRUE(fabs(result.Next().ReadFloat(0) - 4.1522) < 0.0001);
    EXPECT_FALSE(result.Next());
  }
  db = nullptr;
  std::filesystem::remove("__tmp0114");
}

TEST(ExecutorDistinctTest, SmallTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0109");
  auto db = std::make_unique<wing::Instance>("__tmp0109", SAKURA_USE_JIT_FLAG);
  // empty return set
  {
    // clang-format off
    auto result = db->Execute("select distinct a, b, c from (values(1), (2), (3)) _(a), (values(1), (2), (3)) _(b), (values(1), (2), (3)) _(c) order by a asc, b desc, c asc limit 10 offset 30;");
    // clang-format on
    EXPECT_TRUE(result.Valid());
    EXPECT_FALSE(result.Next());
  }
  {
    // clang-format off
    auto result = db->Execute("select distinct a, b, c from (values(1), (1), (1)) _(a), (values(1), (2), (3)) _(b), (values(1), (2), (3)) _(c) order by a asc, b desc, c asc;");
    // clang-format on
    EXPECT_TRUE(result.Valid());
    AnsVec answer;
    for (int i = 1; i <= 1; i++)
      for (int j = 3; j >= 1; --j)
        for (int k = 1; k <= 3; k++)
          answer.emplace_back(
              MkVec(IV::Create(1), IV::Create(j), IV::Create(k)));
    CHECK_ALL_SORTED_ANS(answer, result, 3);
  }
  {
    // clang-format off
    auto result = db->Execute("select distinct a, b, c from (values(1), (1), (1)) _(a), (values(2), (2), (2)) _(b), (values('a'), ('a'), ('a')) _(c) order by a asc, b desc, c asc;");
    // clang-format on
    EXPECT_TRUE(result.Valid());
    AnsVec answer;
    answer.emplace_back(MkVec(IV::Create(1), IV::Create(2), SV::Create("a")));
    CHECK_ALL_SORTED_ANS(answer, result, 3);
  }
  // empty string.
  {
    // clang-format off
    auto result = db->Execute("select distinct * from (values(''), (''), ('a')) _(a);");
    // clang-format on
    EXPECT_TRUE(result.Valid());
    AnsVec answer;
    answer.emplace_back(MkVec(SV::Create("")));
    answer.emplace_back(MkVec(SV::Create("a")));
    CHECK_ALL_SORTED_ANS(answer, result, 1);
  }
  db = nullptr;
  std::filesystem::remove("__tmp0109");
}

TEST(ExecutorDistinctTest, BigTest) {
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0115");
  auto db = std::make_unique<wing::Instance>("__tmp0115", SAKURA_USE_JIT_FLAG);
  // Get distinct code lines from a big code base.
  // Clearly there are many duplicate codes.
  {
    EXPECT_TRUE(db->Execute("create table CodeBase(code varchar(80), line "
                            "int64, file_name varchar(20));")
                    .Valid());
    int NUM = 2e5, DISTINCT_NUM = 1e4;
    std::vector<std::string> codelines;
    std::mt19937_64 rgen(0x202304111547ll);
    for (int i = 0; i < DISTINCT_NUM; i++) {
      int len = rgen() % 80;
      char char_set[] =
          "abcdefghijklmnopqrstuvwxyzQWERTYUIOPLKJHGFDSANMZXCVB?:{[1234567890-="
          "/.,!@#$%^&*()]}";
      std::string new_code(len, 0);
      for (int j = 0; j < len; j++) {
        new_code[j] = char_set[rgen() % sizeof(char_set)];
      }
      codelines.push_back(std::move(new_code));
    }
    std::sort(codelines.begin(), codelines.end());
    codelines.erase(
        std::unique(codelines.begin(), codelines.end()), codelines.end());
    std::shuffle(codelines.begin(), codelines.end(), rgen);
    {
      std::string stmt = "insert into CodeBase values ";
      size_t line = 0;
      for (int i = 0; i < NUM; i++) {
        stmt += fmt::format("('{}', {}, 'a.cpp')",
            codelines[rgen() % codelines.size()], ++line);
        if (i != NUM - 1)
          stmt += ",";
      }
      stmt += ";";
      EXPECT_TRUE(db->Execute(stmt).Valid());
    }
    std::sort(codelines.begin(), codelines.end());
    {
      ResultSet result;
      StopWatch sw;
      EXPECT_TRUE(test_timeout(
          [&]() {
            result = db->Execute(
                "select distinct * from (select code from CodeBase) order by "
                "code asc;");
          },
          3000));
      DB_INFO("Use: {} s", sw.GetTimeInSeconds());

      AnsVec answer;
      for (auto& a : codelines)
        answer.emplace_back(MkVec(SV::Create(a)));
      CHECK_ALL_SORTED_ANS(answer, result, 1);
    }

    {
      ResultSet result;
      StopWatch sw;
      EXPECT_TRUE(test_timeout(
          [&]() {
            result = db->Execute("select distinct file_name from CodeBase;");
          },
          1000));
      DB_INFO("Use: {} s", sw.GetTimeInSeconds());
      AnsVec answer;
      answer.emplace_back(MkVec(SV::Create("a.cpp")));
      CHECK_ALL_SORTED_ANS(answer, result, 1);
    }
  }
  db = nullptr;
  std::filesystem::remove("__tmp0115");
}

TEST(ExecutorAllTest, OJContestTest) {
  // In Lecture 2
  using namespace wing;
  using namespace wing::wing_testing;
  std::filesystem::remove("__tmp0110");
  auto db = std::make_unique<wing::Instance>("__tmp0110", SAKURA_USE_JIT_FLAG);
  // clang-format off
  EXPECT_TRUE(db->Execute("create table Contestant(id int32 auto_increment primary key, name varchar(8));").Valid());
  EXPECT_TRUE(db->Execute("create table Question(id int32 auto_increment primary key, name varchar(8));").Valid());
  EXPECT_TRUE(db->Execute("create table Contest(id int32, problem_rk int32, question_id int32 foreign key references Question(id));").Valid());
  EXPECT_TRUE(db->Execute("create table Submission(id int32 auto_increment primary key, contestant_id int32 foreign key references Contestant(id), question_id int32 foreign key references Question(id), score int32);").Valid());
  // clang-format on
  int CNUM = 1e3;   // Contestant size
  int QNUM = 1e3;   // Question size
  int SNUM = 1e6;   // Submission size
  int CTNUM = 100;  // Contest size
  RandomTuple<int32_t, std::string> tuple_gen(202303040114ull, 0, 0, 7, 8);
  auto [stmt_c, data_c] = tuple_gen.GenerateValuesClause(CNUM);
  EXPECT_TRUE(db->Execute("insert into Contestant " + stmt_c + ";").Valid());
  auto [stmt_q, data_q] = tuple_gen.GenerateValuesClause(QNUM);
  EXPECT_TRUE(db->Execute("insert into Question " + stmt_q + ";").Valid());
  // In OI, the full score is 100.
  RandomTuple<int32_t, int32_t, int32_t, int32_t> tuple_gen2(
      202303040123, 0, 0, 1, CNUM, 1, QNUM, 0, 100);
  auto [stmt_s, data_s] = tuple_gen2.GenerateValuesClause(SNUM);
  EXPECT_TRUE(db->Execute("insert into Submission " + stmt_s + ";").Valid());

  std::vector<std::vector<int>> contests;
  {
    std::mt19937_64 gen(202303040120ull);
    std::uniform_int_distribution<> dis(1, QNUM);
    for (int i = 0; i < CTNUM; i++) {
      // In OI, every contest has 3 problems.
      // But in ACM, contests may have >10 problems.
      int problem_num = gen() % 2 ? 3 : 10;
      contests.emplace_back();
      for (int j = 0; j < problem_num; j++) {
        auto pro = dis(gen);
        contests.back().push_back(pro);
        EXPECT_TRUE(
            db->Execute(fmt::format("insert into Contest values ({}, {}, {});",
                            i, j, pro))
                .Valid());
      }
    }
  }

  // Find the Contestant ID, name, and total score (take the
  // highest score for each question) for every contest ordered by descending
  // score and by ascending ID in the case of a score tie.
  // Only Display the contestants with a total score > 0.

  {
    std::unordered_map<int, std::map<int, int>> scores;
    for (int i = 0; i < SNUM; i++) {
      int qid = data_s.Get(i, 2)->ReadInt();
      int cid = data_s.Get(i, 1)->ReadInt();
      int score = data_s.Get(i, 3)->ReadInt();
      int& x = scores[qid][cid];
      x = std::max(x, score);
    }
    std::vector<AnsVec> answer;
    for (int i = 0; i < CTNUM; i++) {
      std::map<int, int> men;
      for (auto& question : contests[i]) {
        for (auto& [cid, score] : scores[question]) {
          men[cid] += score;
        }
      }
      AnsVec ans;
      std::vector<std::pair<int, int>> A;
      for (auto [cid, score] : men)
        if (score > 0)
          A.emplace_back(cid, score);
      std::sort(A.begin(), A.end(), [](auto x, auto y) {
        if (x.second == y.second)
          return x.first < y.first;
        return x.second > y.second;
      });
      for (auto [cid, score] : A)
        ans.push_back(MkVec(IV::Create(cid),
            SV::Create(data_c.Get(cid - 1, 1)->ReadString()),
            IV::Create(score)));
      answer.push_back(std::move(ans));
    }
    std::vector<ResultSet> result;
    StopWatch sw;
    EXPECT_TRUE(test_timeout(
        [&]() {
          for (int i = 0; i < CTNUM; i++) {
            // clang-format off
            auto ret = db->Execute(fmt::format("select id, name, t_s from (select id, name, sum(S.score) as t_s from (select A.id, A.name, max(B.score) as score from Contestant as A, (select Submission.question_id, Submission.score, Submission.contestant_id, Contest.problem_rk from (select * from Contest where Contest.id = {}) join Submission on Contest.question_id = Submission.question_id) as B where A.id = B.contestant_id group by A.id, B.question_id, problem_rk) as S group by S.name, S.id) where t_s > 0 order by t_s desc, id asc;", i));
            // clang-format on
            result.push_back(std::move(ret));
          }
        },
        20000));
    DB_INFO("Use: {} s", sw.GetTimeInSeconds());
    for (int j = 0; j < CTNUM; j++) {
      CHECK_ALL_SORTED_ANS(answer[j], result[j], 3);
    }
  }

  // Find the top 100 problems on AC rate.
  // AC rate: AC submissions / All submissions.
  {
    ResultSet result;
    std::unordered_map<int, std::pair<int, int>> scores;
    for (int i = 0; i < SNUM; i++) {
      int qid = data_s.Get(i, 2)->ReadInt();
      int score = data_s.Get(i, 3)->ReadInt();
      scores[qid].first += score == 100;
      scores[qid].second++;
    }
    std::vector<std::pair<double, int>> A;
    for (auto [qid, a] : scores)
      A.emplace_back(a.first / (double)a.second, qid);
    std::sort(A.begin(), A.end(), [&](auto& x, auto& y) {
      return x.first == y.first ? x.second < y.second : x.first > y.first;
    });

    AnsVec answer;
    for (int i = 0; i < 100; i++)
      answer.push_back(MkVec(IV::Create(A[i].second),
          SV::Create(data_q.Get(A[i].second - 1, 1)->ReadString()),
          FV::Create(A[i].first)));

    EXPECT_TRUE(test_timeout(
        [&]() {
          // clang-format off
          result = db->Execute("select Question.id, Question.name, rate from (select S.question_id, sum(S.score=100)/(1.0*count(*)) as rate from Submission as S group by S.question_id) A join Question on A.question_id=Question.id order by A.rate desc, Question.id asc limit 100;");
          // clang-format on
        },
        1000));
    CHECK_ALL_SORTED_ANS(answer, result, 3);
  }

  db = nullptr;
  std::filesystem::remove("__tmp0110");
}
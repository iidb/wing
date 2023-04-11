#include <gtest/gtest.h>

#include <filesystem>

#include "instance/instance.hpp"
#include "test.hpp"

TEST(BasicTest, ParserTest) {
  using namespace wing;
  std::filesystem::remove("__tmp-1");
  auto db = std::make_unique<wing::Instance>("__tmp-1", 0);
  // SELECT
  EXPECT_FALSE(db->Execute("select").ParseValid());
  EXPECT_FALSE(db->Execute("select 1").ParseValid());
  EXPECT_TRUE(db->Execute("select 1;").ParseValid());
  EXPECT_TRUE(db->Execute("sElecT 1 where 0;").ParseValid());
  EXPECT_FALSE(db->Execute("sElecT 1 where 0 order by 0;").ParseValid());
  EXPECT_TRUE(db->Execute("sElecT 1 where 0 order by 0 asc;").ParseValid());
  EXPECT_TRUE(db->Execute("select 114514 group by 1;").ParseValid());
  EXPECT_TRUE(db->Execute("select 1 where 0 group by 1;").ParseValid());
  EXPECT_TRUE(db->Execute("select 1 group by 1 order by 0 asc;").ParseValid());
  EXPECT_TRUE(
      db->Execute("select 1 where 0 group by 1 order by 0 asc;").ParseValid());
  EXPECT_TRUE(db->Execute("select 1 group by 1 having 1 > 0 order by 0 asc;")
                  .ParseValid());
  EXPECT_TRUE(
      db->Execute("select 1 where 0 group by 1 having 1 > 0 order by 0 asc;")
          .ParseValid());
  EXPECT_TRUE(db->Execute("select sum(1);").ParseValid());
  EXPECT_TRUE(db->Execute("select max(1);").ParseValid());
  EXPECT_FALSE(db->Execute("select sum(sum(1));").ParseValid());
  EXPECT_FALSE(db->Execute("select sum(max(1));").ParseValid());
  EXPECT_FALSE(db->Execute("select min(max(1));").ParseValid());
  EXPECT_TRUE(db->Execute("select sum(1) where 0;").ParseValid());
  EXPECT_TRUE(db->Execute("select sum(1) order by 0 asc;").ParseValid());
  EXPECT_TRUE(
      db->Execute("select sum(1) where 0 order by 0 asc;").ParseValid());
  EXPECT_FALSE(
      db->Execute("select sum(1) having 1 > 0 order by 0 asc;").ParseValid());
  EXPECT_FALSE(db->Execute("select sum(1) where 0 having 1 > 0 order by 0 asc;")
                   .ParseValid());
  EXPECT_FALSE(db->Execute("select sum(1), a from values(2) as _(a) where 0 "
                           "group by 2 order by 0 asc;")
                   .ParseValid());
  EXPECT_TRUE(db->Execute("select sum(1), a from (values(2)) as _(a) where 0 "
                          "group by 2 order by 0 asc;")
                  .ParseValid());
  EXPECT_TRUE(
      db->Execute(
            "select distinct * from (select distinct * from (values(2)));")
          .ParseValid());
  EXPECT_TRUE(db->Execute("select distinct * from (values(2)) as _(a) order by "
                          "a asc limit 10;")
                  .ParseValid());
  EXPECT_TRUE(
      db->Execute(
            "select * from (values(2,3.0,8,'x'),(4,7.0,8,'y')), (values(5,6)) "
            "as foo(p,q),(values(7,6,'ppp','qq')) where p<7;")
          .ParseValid());
  // Return type of predicate must be integer.
  EXPECT_FALSE(db->Execute("select 0 where 0.0;").ParseValid());
  EXPECT_FALSE(db->Execute("select 0 where \'a\';").ParseValid());
  EXPECT_FALSE(
      db->Execute("select sum(1) from values(2, 3.0) as A(a, b) group by a;")
          .ParseValid());
  EXPECT_FALSE(
      db->Execute("select sum(1) from values(2, '3') as A(a, b) group by a;")
          .ParseValid());
  // CREATE
  EXPECT_FALSE(db->Execute("create;").ParseValid());
  EXPECT_TRUE(db->Execute("create tablE A (a inT64, b int64);").ParseValid());
  EXPECT_TRUE(db->Execute("create tablE \"\" (\"\" inT64);").ParseValid());
  // INSERT
  EXPECT_TRUE(db->Execute("insert into A values(2, 3), (4, 5);").ParseValid());
  EXPECT_FALSE(
      db->Execute("insert into A values(2, 3), (4, 5.0);").ParseValid());
  EXPECT_TRUE(db->Execute("insert into A select * from A;").ParseValid());
  // DELETE
  EXPECT_TRUE(db->Execute("delete from A where a < 3;").ParseValid());
  EXPECT_TRUE(db->Execute("delete from A;").ParseValid());
  EXPECT_FALSE(db->Execute("delete from A where c < 3;").ParseValid());
  // UPDATE
  EXPECT_FALSE(db->Execute("update A;").ParseValid());
  EXPECT_TRUE(db->Execute("update A set A.a = A.a + 1;").ParseValid());
  EXPECT_TRUE(db->Execute("update A set A.a = a + 1;").ParseValid());
  EXPECT_FALSE(db->Execute("update A set A.a = c + 1;").ParseValid());
  // JOIN and SUBQUERY
  EXPECT_TRUE(db->Execute("select sum(a) from A where a = 1 group by b having "
                          "b > 10 order by a asc, b desc limit 1+9 offset 0;")
                  .ParseValid());
  EXPECT_TRUE(db->Execute("select sum(C.a) + sum(C.b) as d from A, A as C;")
                  .ParseValid());
  EXPECT_TRUE(
      db->Execute(
            "select _.d, a from (select sum(C.a) as d from A, A as C) as _, A;")
          .ParseValid());
  EXPECT_TRUE(
      db->Execute(
            "select e, _.e from (select sum(C.a) as d from A, A as C) as _(e);")
          .ParseValid());
  EXPECT_TRUE(
      db->Execute(
            "select * from A join A as C on C.a = A.a join A as D on C.a=D.a;")
          .ParseValid());
  EXPECT_TRUE(db->Execute("select distinct * from (select distinct * from A);")
                  .ParseValid());
  EXPECT_TRUE(
      db->Execute(
            "select distinct * from (select distinct * from A limit 10), A;")
          .ParseValid());
  EXPECT_TRUE(db->Execute("select distinct * from (select distinct * from A "
                          "order by a asc limit 10), A;")
                  .ParseValid());
  EXPECT_FALSE(db->Execute("select distinct * from (select distinct * from A "
                           "limit 10 order by a asc), A;")
                   .ParseValid());
  EXPECT_TRUE(db->Execute("select distinct * from A, A;").ParseValid());
  // DROP
  EXPECT_TRUE(db->Execute("drop tablE \"\";").ParseValid());
  std::filesystem::remove("__tmp-1");
}

TEST(BasicTest, ConstantExprTest) {
  using namespace wing;
  std::filesystem::remove("__tmp0");
  auto db = std::make_unique<wing::Instance>("__tmp0", 0);
  auto test_func_int = [&](auto&& stmt, int64_t expect) {
    auto result = db->Execute(stmt);
    if (!result.Valid()) {
      DB_INFO("{}", result.GetErrorMsg());
    }
    ASSERT_TRUE(result.Valid());
    EXPECT_EQ(result.Next().ReadInt(0), expect);
    EXPECT_FALSE(bool(result.Next()));
  };
  auto test_func_float = [&](auto&& stmt, double expect) {
    auto result = db->Execute(stmt);
    ASSERT_TRUE(result.Valid());
    EXPECT_EQ(result.Next().ReadFloat(0), expect);
    EXPECT_FALSE(bool(result.Next()));
  };
  test_func_int("select 1;", 1);
  test_func_int("select -1;", -1);
  test_func_int("select 2*5/10;", 1);
  test_func_int("select 2*5/-10;", -1);
  test_func_int("select -----5--6;", 1);
  test_func_int("select (2*3-4)*6;", 12);
  test_func_int("select 2147483648;", 2147483648);
  test_func_int("select 9223372036854775807;", 9223372036854775807ull);
  test_func_int("select -9223372036854775808;", -9223372036854775808ull);
  test_func_int("select 1<2;", 1);
  test_func_int("select 1>2;", 0);
  test_func_int("select 1=1;", 1);
  test_func_int("select 1<>1;", 0);
  test_func_int("select 2>=2;", 1);
  test_func_int("select 2<=2;", 1);
  test_func_int("select 2>=3;", 0);
  test_func_int("select 2<=1;", 0);
  test_func_int("select 1<2.0;", 1);
  test_func_int("select 1.0>2;", 0);
  test_func_int("select 1.0=1.0;", 1);
  test_func_int("select 1<>1.0;", 0);
  test_func_int("select 2>=2.0;", 1);
  test_func_int("select 2<=2.0;", 1);
  test_func_int("select 2.0>=3.0;", 0);
  test_func_int("select 2.0<=1;", 0);
  test_func_int("select 1+2<3;", 0);
  test_func_int("select 1+2>3;", 0);
  test_func_int("select 6*8<3&63;", 0);
  test_func_int("select 6*8>3|63;", 0);
  test_func_int("select 8<>6^8^6;", 0);
  test_func_int("select 8=6^8^6;", 1);
  test_func_int(
      "select 2*3*4*5*6*7*8*9*10*11*12*14*13*15%998244353;", 972509923);
  test_func_int(
      "select "
      "11086+21474*27493-497*23281-7452*5157+11278-19251*20852-24255+(27912-"
      "16387*(2901+29195*(1148*((19110+1635*((205+20751+15285-((13349+(4817-(("
      "29367-(4698+9970-12619-(8520*1225*7967*((12910-9024+32252-(18025*(8269*"
      "4309+(11441)*28737))+28276)))))))+26542))))))+29521*3826)+1552)+31591)+"
      "10907*30881;",
      -3482772898561652509);
  test_func_int(
      "select "
      "(28124^32396^15165^6126-(6794|3102%(26931&(30401^30654%31396^(((14057|("
      "29319%11921+21840*((8053|8862%(32306|4855-10504-2821+((9031|925^(4212%("
      "(18568%(((28296^11840^31196^(15145|1688^19171-(17084-(31440+4389+(17629%"
      "20763*8757%16194+((29027%(31757|15371%30978-26193))))))))))^(21925)^"
      "15927))))))))-15449)))))))|10322);",
      -1665);
  test_func_int(
      "select "
      "27930|2358+26123|8828-(10047&5824&25451-13862|30595-9109-21199&(22326-"
      "30708&14828*1779*10708*21887+(1540|22078+19438+16593*27446*25527|19686*"
      "5740-12602*13639*(2943^14860*30135*13104|17927-24890+19984^27852-26823*"
      "18736&17772^16009*26282|5103|8966-(13602&10283+18589*9397-18977|23683*"
      "13090-(6589-21908*14610+7940+2519-20750*29452*10626*((21628|18399-32615*"
      "9966+29816^4246+25859+(22350*21422-(11190-11546&26164|23335*25526|"
      "15848&(29148&11105+2621*11165*5511&((10452&28317-19330^13077*18630&"
      "27024^5481*30695-19026|6704-8025&22773-18357|12347+16249&557*19419)|"
      "23848*"
      "5575))*20375&12911))-28399*19596))-4703)*15051)&16239+(4618^(11353)*"
      "10811|4059)&10092)^20674+4814*22787+7365)|29040-24849+27737-31996&12965)"
      "-"
      "32319&23351+7013^25344|28782)^25948;",
      -4261);
  // FLOAT
  test_func_float("select 1.0;", 1.0);
  test_func_float("select 1e20;", 1e20);
  test_func_float("select 1E20;", 1e20);
  test_func_float("select 1E+20;", 1e20);
  test_func_float("select 1E-20;", 1e-20);
  test_func_float("select -1E-20;", -1e-20);
  test_func_float("select ---1E+20;", -1e20);
  test_func_float(
      "select "
      "-(((((234&459+34095*349)|6598^23409+95-239842)-(3498)%239+(449./"
      "2)+938%98/2*(834+59)+43.)+(43452&9583|9764^358<<5|2<<4))-(23554./"
      "(345246/"
      "34.))-(596.+4./(45./34.)));",
      182371.84183200772);
  test_func_int("select 'a' < 'b';", 1);
  test_func_int("select 'a' > 'b';", 0);
  test_func_int("select 'a' <= 'a';", 1);
  test_func_int("select 'a' >= 'a';", 1);
  test_func_int("select 'a' <= 'b';", 1);
  test_func_int("select 'a' >= 'b';", 0);
  test_func_int("select 'b' <= 'a';", 0);
  test_func_int("select 'b' >= 'a';", 1);
  test_func_int("select not ('a' > 'b');", 1);
  test_func_int("select not ('a' > 'b') and ('ccc' < 'd');", 1);
  test_func_int("select 'aasdfasd' <> 'aasdfasd';", 0);
  test_func_int("select 'sakura' <> 'pp32or';", 1);
  test_func_int(
      "select 'aasdfasd' <> 'aasdfasd' and 'sakura' <> 'syaoran';", 0);
  test_func_int(
      "select 'aasdfasd' = 'aasdfasd' and 'sakura' <> 'syaoran' and 'pp' > "
      "'pe' and 'oee' < 'oeep';",
      1);
  test_func_int(
      "select 'aasdfasd' <> 'aasdfasd' or 'sakura' = 'syaoran' or 'pp' <'pe' "
      "or 'oee' > 'oeep';",
      0);
  test_func_int("select 0 and 0 or 1;", 1);
  test_func_int("select 0 and (0 or 1);", 0);
  test_func_int("select not 5;", 0);
  test_func_int("select not 1 < 2;", 1);
  test_func_int("select not (1 < 2);", 0);
  db = nullptr;
  std::filesystem::remove("__tmp0");
}

TEST(BasicTest, Project) {
  using namespace wing;
  std::filesystem::remove("__tmp1");
  auto db = std::make_unique<wing::Instance>("__tmp1", SAKURA_USE_JIT_FLAG);
  db->Execute(
      "create table A (a1 int64, a2 int32, a3 float64, a4 varchar(30), a5 "
      "int32, a6 int32, a7 varchar(1), a8 varchar(2), a9 float64, a10 int64, "
      "a11 "
      "varchar(255), a12 varchar(1), a13 varchar(255));");
  wing_testing::RandomTuple<int64_t, int32_t, double, std::string, int32_t,
      int32_t, std::string, std::string, double, int64_t, std::string,
      std::string, std::string>
      tuple_gen(114514, INT64_MIN, INT64_MAX, INT32_MIN, INT32_MAX, -1.0, 1.0,
          0, 30, 1, 100, INT32_MIN, INT32_MAX, 0, 1, 0, 2, 0.0, 1e20, 0, 10,
          255, 255, 0, 0, 255, 255);
  size_t PAIR_NUM = 1e4;
  auto data = tuple_gen.GenerateValuesClause(PAIR_NUM);

  {
    std::string stmt = "insert into A " + data.first + ";";
    auto result = db->Execute(stmt);
    EXPECT_TRUE(result.Valid());
    auto tuple = result.Next();
    EXPECT_TRUE(bool(tuple));
    EXPECT_EQ(tuple.ReadInt(0), PAIR_NUM);
    tuple = result.Next();
    EXPECT_FALSE(bool(tuple));
  }

  std::vector<size_t> sorted_id;
  sorted_id.resize(PAIR_NUM);
  std::iota(sorted_id.begin(), sorted_id.end(), 0);

  std::sort(sorted_id.begin(), sorted_id.end(), [&](auto x, auto y) {
    return data.second.Get(x, 0)->ReadInt() < data.second.Get(y, 0)->ReadInt();
  });

  {
    auto result = db->Execute(
        "select a12, a1, a4, a11, a7, a3, a2 from (select a7, a2, a3, a4, a6, "
        "a1, a11, a12 from (select * from A));");
    EXPECT_TRUE(result.Valid());
    for (uint32_t i = 0; i < PAIR_NUM; i++) {
      auto tuple = result.Next();
      EXPECT_TRUE(bool(tuple));
      uint32_t id = 0;
      for (uint32_t j = (1u << 30); j >= 1; j >>= 1) {
        if (j + id < PAIR_NUM &&
            tuple.ReadInt(1) >=
                data.second.Get(sorted_id[j + id], 0)->ReadInt())
          id += j;
      }
      auto pair = data.second.GetTuple(sorted_id[id]);
      EXPECT_EQ(tuple.ReadString(0), pair[11]->ReadString());
      EXPECT_EQ(tuple.ReadInt(1), pair[0]->ReadInt());
      EXPECT_EQ(tuple.ReadString(2), pair[3]->ReadString());
      EXPECT_EQ(tuple.ReadString(3), pair[10]->ReadString());
      EXPECT_EQ(tuple.ReadString(4), pair[6]->ReadString());
      EXPECT_EQ(tuple.ReadFloat(5), pair[2]->ReadFloat());
      EXPECT_EQ(tuple.ReadInt(6), pair[1]->ReadInt());
    }
    auto tuple = result.Next();
    EXPECT_FALSE(bool(tuple));
  }

  EXPECT_TRUE(
      db->Execute(
            "create table B(b1 int64, b2 int32, b3 varchar(255), b4 float64);")
          .Valid());
  EXPECT_TRUE(db->Execute("insert into B select a1, a2, a4, a3 from A where a1 "
                          "% 2 = 0 and a2 % 2 = 0;")
                  .Valid());

  {
    auto result = db->Execute(
        "select b1, b2-b1|1/3*2%998244353+b1*b1/-(b1+-b2)^94832-(-b2-b1)^99, "
        "b1%20<b1%20+b2%20, b1*2/5, b1+b4, b1*b4, b2-(b1+b4), "
        "b1%10|2<b2%10,"
        "b1%10>=b2&3%10, b1-(b2%5<=b1%-5), (0<b1%2)+(b2%6 < 3 and b1%3<b2%3 or "
        "b1%4<2),"
        "b1%10<>b2%10, b1%10=b2%10, b1%10>b2%10, b1%10<b2%10, b1%10>=b2%10, "
        "b1%5<=b2%5, b3, (b3 < 'bp')+ (b3=b3), b3 from B where b1 % 100 < 90;");
    EXPECT_TRUE(result.Valid());
    std::vector<std::vector<std::unique_ptr<wing_testing::Value>>> answer;
    for (size_t i = 0; i < PAIR_NUM; i++) {
      auto a = data.second.GetTuple(i);
      if (a[0]->ReadInt() % 2 == 0 && a[1]->ReadInt() % 2 == 0 &&
          a[0]->ReadInt() % 100 < 90) {
        auto b1 = a[0]->ReadInt();
        auto b2 = a[1]->ReadInt();
        auto b3 = a[3]->ReadString();
        auto b4 = a[2]->ReadFloat();
        std::vector<std::unique_ptr<wing_testing::Value>> T;
        T.push_back(wing_testing::IntValue::Create(b1));
        T.push_back(wing_testing::IntValue::Create(
            (b2 - b1) | ((1 / 3 * 2 % 998244353 + b1 * b1 / -(b1 + -b2)) ^
                            (94832 - (-b2 - b1)) ^ 99)));
        T.push_back(
            wing_testing::IntValue::Create(b1 % 20 < b1 % 20 + b2 % 20));
        T.push_back(wing_testing::IntValue::Create(b1 * 2 / 5));
        T.push_back(wing_testing::FloatValue::Create(b1 + b4));
        T.push_back(wing_testing::FloatValue::Create(double(b1) * b4));
        T.push_back(wing_testing::FloatValue::Create(b2 - (b1 + b4)));
        T.push_back(wing_testing::IntValue::Create((b1 % 10 | 2) < b2 % 10));
        T.push_back(wing_testing::IntValue::Create(b1 % 10 >= (b2 & 3) % 10));
        T.push_back(wing_testing::IntValue::Create(b1 - (b2 % 5 <= b1 % -5)));
        T.push_back(wing_testing::IntValue::Create(
            (0 < b1 % 2) + ((b2 % 6 < 3 && b1 % 3 < b2 % 3) || b1 % 4 < 2)));
        T.push_back(wing_testing::IntValue::Create(b1 % 10 != b2 % 10));
        T.push_back(wing_testing::IntValue::Create(b1 % 10 == b2 % 10));
        T.push_back(wing_testing::IntValue::Create(b1 % 10 > b2 % 10));
        T.push_back(wing_testing::IntValue::Create(b1 % 10 < b2 % 10));
        T.push_back(wing_testing::IntValue::Create(b1 % 10 >= b2 % 10));
        T.push_back(wing_testing::IntValue::Create(b1 % 5 <= b2 % 5));
        T.push_back(wing_testing::StringValue::Create(b3));
        T.push_back(wing_testing::IntValue::Create((b3 < "bp") + (b3 == b3)));
        T.push_back(wing_testing::StringValue::Create(b3));
        answer.push_back(std::move(T));
      }
    }
    std::sort(answer.begin(), answer.end(), [&](const auto& x, const auto& y) {
      return x[0]->ReadInt() < y[0]->ReadInt();
    });

    for (uint32_t i = 0; i < answer.size(); i++) {
      auto tuple = result.Next();
      EXPECT_TRUE(bool(tuple));

      uint32_t id = 0;
      for (uint32_t j = (1u << 30); j >= 1; j >>= 1) {
        if (j + id < answer.size() &&
            tuple.ReadInt(0) >= answer[j + id][0]->ReadInt())
          id += j;
      }
      for (uint32_t j = 0; j < answer[id].size(); j++) {
        EXPECT_TRUE(wing_testing::TestEQ(tuple, j, answer[id][j].get()));
      }
    }
    auto tuple = result.Next();
    EXPECT_FALSE(bool(tuple));
  }
  db = nullptr;
  std::filesystem::remove("__tmp1");
}

TEST(BasicTest, Save) {
  using namespace wing;
  std::filesystem::remove("__tmp2");
  {
    // Empty DB
    auto db = std::make_unique<wing::Instance>("__tmp2", 0);
    db = nullptr;
    db = std::make_unique<wing::Instance>("__tmp2", 0);
  }
  {
    // Empty Tables
    auto db = std::make_unique<wing::Instance>("__tmp2", 0);
    EXPECT_TRUE(db->Execute("create table A(a int64, b float64, c varchar(1));")
                    .Valid());
    EXPECT_TRUE(db->Execute("create table B(a float64, b int64, c varchar(1));")
                    .Valid());
    EXPECT_TRUE(db->Execute("create table _(a varchar(1), b float64, c int64);")
                    .Valid());
    EXPECT_TRUE(
        db->Execute(
              "create table \" \"(a varchar(1), \"\" float64, \" \" int64);")
            .Valid());
    db = nullptr;
    db = std::make_unique<wing::Instance>("__tmp2", 0);
    // All tables are empty.
    EXPECT_FALSE(bool(db->Execute("select * from A;").Next()));
    EXPECT_FALSE(bool(db->Execute("select * from B;").Next()));
    EXPECT_FALSE(bool(db->Execute("select * from _;").Next()));
    EXPECT_FALSE(bool(db->Execute("select * from \" \";").Next()));
    EXPECT_TRUE(db->Execute("drop table A;").Valid());
    EXPECT_TRUE(db->Execute("drop table B;").Valid());
    EXPECT_TRUE(db->Execute("drop table _;").Valid());
    EXPECT_TRUE(db->Execute("drop table \" \";").Valid());
    db = nullptr;
    // Dropped table cannot be found.
    db = std::make_unique<wing::Instance>("__tmp2", 0);
    EXPECT_FALSE(db->Execute("drop table A;").Valid());
    EXPECT_FALSE(db->Execute("drop table B;").Valid());
    EXPECT_FALSE(db->Execute("drop table _;").Valid());
    EXPECT_FALSE(db->Execute("drop table \" \";").Valid());
  }
  {
    // Tables
    auto db = std::make_unique<wing::Instance>("__tmp2", 0);
    EXPECT_TRUE(
        db->Execute("create table \"\"(a0 int64, \"\" varchar(1), a1 float64, "
                    "\"<<>\" int64, c varchar(255), d float64);")
            .Valid());
    EXPECT_TRUE(
        db->Execute("create table \"<<>\"(a0 int64, d char(2), \"<<>\" int32);")
            .Valid());
    auto PAIR_NUM = 1e4;
    {
      wing_testing::RandomTuple<int64_t, std::string, double, int64_t,
          std::string, double>
          tuple_gen(114515, INT64_MIN, INT64_MAX, 0, 1, -1e300, 1e300,
              INT64_MIN, INT64_MAX, 254, 255, 0.0, 0.0);

      wing_testing::RandomTuple<int64_t, std::string, int32_t> tuple_gen2(
          114516, INT64_MIN, INT64_MAX, 1, 2, INT32_MIN, INT32_MAX);
      auto [stmt, value_1] = tuple_gen.GenerateValuesClause(PAIR_NUM);
      {
        auto result = db->Execute("insert into \"\" " + stmt + ";");
        EXPECT_TRUE(result.Valid());
        EXPECT_EQ(result.Next().ReadInt(0), PAIR_NUM);
      }

      auto [stmt2, value_2] = tuple_gen2.GenerateValuesClause(PAIR_NUM);
      {
        auto result = db->Execute("insert into \"<<>\" " + stmt2 + ";");
        EXPECT_TRUE(result.Valid());
        EXPECT_EQ(result.Next().ReadInt(0), PAIR_NUM);
      }
      // Test if there exists overflow in metadata.
      for (int i = 0; i < 1000; i++) {
        EXPECT_TRUE(
            db->Execute(
                  fmt::format(
                      "create table _{}(a varchar(1), b float64, c int64);", i))
                .Valid());
      }
      db = nullptr;
      auto db = std::make_unique<wing::Instance>("__tmp2", 0);
      {
        auto result =
            db->Execute("select a0, \"\", a1, \"<<>\", c, d from \"\";");
        EXPECT_TRUE(result.Valid());
        std::map<size_t, size_t> st;
        for (size_t i = 0; i < PAIR_NUM; i++) {
          auto id = value_1.Get(i, 0)->ReadInt();
          if (st.find(id) != st.end()) {
            DB_INFO("fuck");
          }
          st[id] = i;
        }
        for (size_t i = 0; i < PAIR_NUM; i++) {
          auto tuple = result.Next();
          EXPECT_TRUE(bool(tuple));
          auto id = st[tuple.ReadInt(0)];
          for (uint32_t j = 0; j < value_1.GetTupleSize(); j++) {
            EXPECT_TRUE(
                wing_testing::TestEQ(tuple, j, value_1.Get(id, j).get()));
          }
        }
        EXPECT_FALSE(bool(result.Next()));
      }

      {
        auto result = db->Execute("select a0, d, \"<<>\" from \"<<>\";");
        EXPECT_TRUE(result.Valid());
        std::map<size_t, size_t> st;
        for (size_t i = 0; i < PAIR_NUM; i++) {
          auto id = value_2.Get(i, 0)->ReadInt();
          if (st.find(id) != st.end()) {
            DB_INFO("fuck");
          }
          st[id] = i;
        }
        for (size_t i = 0; i < PAIR_NUM; i++) {
          auto tuple = result.Next();
          EXPECT_TRUE(bool(tuple));
          auto id = st[tuple.ReadInt(0)];
          for (uint32_t j = 0; j < value_2.GetTupleSize(); j++) {
            EXPECT_TRUE(
                wing_testing::TestEQ(tuple, j, value_2.Get(id, j).get()));
          }
        }
        EXPECT_FALSE(bool(result.Next()));
      }
      for (int i = 0; i < 1000; i++) {
        EXPECT_TRUE(db->Execute(fmt::format("drop table _{};", i)).Valid());
      }
    }
  }
  std::filesystem::remove("__tmp2");
}

TEST(BasicTest, ForeignKey) {
  using namespace wing;
  std::filesystem::remove("__tmp3");
#define CHECKT(str) EXPECT_TRUE(db->Execute(str).Valid());
#define CHECKF(str) EXPECT_FALSE(db->Execute(str).Valid());
  {
    auto db = std::make_unique<wing::Instance>("__tmp3", 0);
    // Auto_increment fields can only be integers.
    CHECKF("create table A(a varchar(20) auto_increment primary key);");
    CHECKF("create table A(a float64 auto_increment primary key);");
    CHECKT("create table A(a int64);");
    CHECKT("insert into A values(2);");
    CHECKT("select * from A;");
    CHECKT("insert into A select * from A;");
    CHECKT("select * from A;");
    CHECKT(
        "create table B(a int64, b varchar(20), c int64 auto_increment primary "
        "key);");
    CHECKT("insert into B values(5, 'fjtk', 0);");
    CHECKT("insert into B values(10086, 'knmte2323re', 0);");
    CHECKT("select * from B;");
    // If the auto_increment field > 0 then it's not managed by DB. So failed.
    CHECKF("insert into B select * from B;");

    CHECKT(
        "create table C(a float64, b varchar(20), c varchar(50), d int64 "
        "foreign key references B(c));");
    CHECKT("insert into C values(0.866, 'ggtd999', 'mhsjmdkPAhamgical', 1);");
    CHECKT("insert into C values(0.899, 'ggtd888', 'mhsjmdkPAhamgical2', 2);");
    CHECKT("insert into C values(0.5, 'ggtd777', 'mhsjmdkPAhamgical3', 1);");
    // B doesn't have primary key 3.
    CHECKF("insert into C values(0.5, 'ggtd777', 'mhsjmdkPAhamgical3', 3);");
    // 1 is referred in C.
    CHECKF("delete from B where c = 1;");
    CHECKT("insert into B values(12306, 'makoan', 3);");
    CHECKT("insert into C values(0.5, 'ggtd777', 'mhsjmdkPAhamgical3', 3);");
    CHECKT("delete from C where d = 2;");
    CHECKT("delete from B where c = 2;");

    CHECKT("insert into B values(10001, 'k5126', 4);");
    CHECKT(
        "create table D(s varchar(50), a int64 foreign key references B(c), b "
        "int64 primary key foreign key references B(c));");
    CHECKT("drop table D;");

    CHECKT(
        "create table D(s varchar(50), a int64 foreign key references B(c), b "
        "int64 primary key foreign key references B(c));");
    CHECKT(
        "insert into D values('sorehamukashinaitayumenokakeranoyou', 4, 1);");
    // the 3-rd field is primary key so it's unique. There is already 1 here.
    CHECKF("insert into D values('minnarigatou', 1, 1);");
    CHECKT(
        "insert into D values('sorehamukashinaitayumenokakeranoyou', 1, 4);");
  }

  {
    auto db = std::make_unique<wing::Instance>("__tmp3", 0);

    // There are some keys are referred in C.
    CHECKF("drop table B;");
    CHECKT("drop table C;");
    // There are some keys are referred in D.
    CHECKF("drop table B;");
    CHECKT("drop table D;");
    // This gets success after Loggers are implemented.
    // CHECKT("drop table B;");
    CHECKT("drop table A;");

    CHECKT("create table A(a varchar(50) primary key, c float64);");
    CHECKT("insert into A values('3', 3.4);");
    CHECKF("insert into A values('3', 5.6);");
    CHECKT("delete from A where a = '3';");
    CHECKT("insert into A values('3', 5.7);");
    CHECKT("insert into A values('x3', 5.7);");

    // The type or size is not the same as A.
    CHECKF(
        "create table C(a varchar(40) foreign key references A(a), c int64, b "
        "varchar(20));");
    CHECKF(
        "create table C(a int64 foreign key references A(a), c int64, b "
        "varchar(20));");
    CHECKT(
        "create table C(a varchar(50) foreign key references A(a), c int64, b "
        "varchar(20));");
    CHECKF("insert into C values ('xd', 3, 'ddd');");
    CHECKT("insert into C values ('x3', 999, '0932');");
    CHECKF("insert into C values ('a3', 999, '0932');");
    CHECKF("delete from A;");
    CHECKT("drop table C;");
    CHECKT("drop table A;");
  }

#undef CHECKT
#undef CHECKF
  std::filesystem::remove("__tmp3");
}
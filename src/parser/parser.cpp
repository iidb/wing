#include "parser/parser.hpp"

#include <charconv>
#include <chrono>
#include <map>
#include <numeric>
#include <optional>

#include "execution/exprdata.hpp"
#include "fmt/format.h"
#include "parser/ast.hpp"
#include "plan/plan.hpp"
#include "type/field.hpp"

namespace wing {

enum class TokenType {
  SELECT,
  FROM,
  WHERE,
  INSERT,
  INTO,
  VALUES,
  UPDATE,
  SET,
  DELETE,
  CREATE,
  DROP,
  TABLE,
  PRIMARY,
  FOREIGN,
  REFERENCES,
  KEY,
  AUTOINC,
  INTEGER,
  INT32,
  INT64,
  REAL,
  FLOAT64,
  CHAR,
  VARCHAR,
  INDEX,
  VIEW,
  LIMIT,
  OFFSET,
  ASC,
  DESC,
  GROUP,
  ORDER,
  JOIN,
  INNER,
  ON,
  BY,
  DISTINCT,
  HAVING,
  AS,
  MAX,
  MIN,
  SUM,
  AVG,
  COUNT,
  _STAR,
  _DOT,
  _COMMA,
  _SEMICOLON,
  _LEFTQ,
  _RIGHTQ,
  _AND,
  _NOT,
  _OR,
  _OPERATOR,
  _LITERIAL_STRING,
  _LITERIAL_INTEGER,
  _LITERIAL_INTEGER_OVERFLOW,
  _LITERIAL_FLOAT,
  _LITERIAL_FLOAT_OVERFLOW,
  _LITERIAL_INVALID_STRING,
  _LITERIAL_INVALID_NUMBER,
  _TABLENAME,
  _TABLENAME_INVALID,
  _NONE,
  _END
};

static const char* all_tokens[] = {"select", "from", "where", "insert", "into",
    "values", "update", "set", "delete", "create", "drop", "table", "primary",
    "foreign", "references", "key", "auto_increment", "integer", "int32",
    "int64", "real", "float64", "char", "varchar", "index", "view", "limit",
    "offset", "asc", "desc", "group", "order", "join", "inner", "on", "by",
    "distinct", "having", "as", "max", "min", "sum", "avg", "count", "*", ".",
    ",", ";", "(", ")", "and", "not", "or", "+", "-", "/", "%", ">", "<", "^",
    "&", "|", "=", ">=", "<=", "<>", "<<", ">>"};

template <const uint32_t SZ, const uint32_t A>
class Trie {
  uint8_t next_[A][SZ];
  uint8_t val_[SZ];
  uint32_t size_;

 public:
  Trie() {
    std::memset(next_, 0, SZ * A * sizeof(uint8_t));
    std::memset(val_, 0xff, SZ * sizeof(uint8_t));
    size_ = 1;
  }
  void Insert(std::string_view s, uint32_t val) {
    uint32_t rt = 0;
    for (auto& ch : s) {
      uint32_t a = static_cast<uint32_t>(ch);
      if (!next_[a][rt]) {
        next_[a][rt] = size_++;
        DB_ASSERT(size_ <= SZ);
      }
      rt = next_[a][rt];
    }
    val_[rt] = val;
  }
  int Search(std::string_view s) {
    uint32_t rt = 0;
    for (auto& ch : s) {
      uint32_t a = static_cast<uint32_t>(ch);
      if (a > 128)
        break;
      if (std::isalpha(a))
        a = std::tolower(a);
      if (!next_[a][rt])
        break;
      rt = next_[a][rt];
    }
    if (!rt)
      return -1;
    return val_[rt] == 0xff ? -1 : val_[rt];
  }
};

/**
 * Convert string to double.
 * Since std::stod is too slow.
 */

double StringToDouble(std::string_view str, double sign = 1) {
  double value = 0;
  double scale = 1;
  uint32_t i = 0;
  for (; i < str.size() && std::isdigit(str[i]); i++) {
    value = value * 10 + str[i] - '0';
  }
  if (i < str.size() && str[i] == '.') {
    double p10 = 1;
    i += 1;
    double v2 = 0;
    while (i < str.size() && std::isdigit(str[i])) {
      v2 = v2 * 10 + (str[i] - '0');
      p10 *= 10.0;
      i += 1;
    }
    value += v2 / p10;
  }

  if (!std::isfinite(value)) {
    throw std::out_of_range("");
  }

  bool frac = 0;
  if (i < str.size() && (str[i] == 'e' || str[i] == 'E')) {
    i += 1;
    if (i < str.size() && str[i] == '+') {
      frac = 0;
      i += 1;
    } else if (i < str.size() && str[i] == '-') {
      frac = 1;
      i += 1;
    }
    size_t expon = 0;
    for (; i < str.size() && std::isdigit(str[i]); i++) {
      expon = expon * 10 + str[i] - '0';
      if (expon > 308) {
        throw std::out_of_range("");
      }
    }
    while (expon >= 50) {
      scale *= 1e50;
      expon -= 50;
    }
    while (expon >= 8) {
      scale *= 1e8;
      expon -= 8;
    }
    while (expon > 0) {
      scale *= 10.0;
      expon -= 1;
    }
  }
  auto ret = sign * (frac ? (value / scale) : (value * scale));
  if (!std::isfinite(ret)) {
    throw std::out_of_range("");
  }
  if (i != str.size() || std::isnan(ret)) {
    throw std::invalid_argument("");
  }
  return ret;
}

/**
 * Convert string to int.
 */

int64_t StringToInt(std::string_view str, int sign = 1) {
  int64_t ret = 0;
  if (str.size() > 19) {
    throw std::out_of_range("");
  }
  if (str.size() == 19) {
    const char* MAX = "9223372036854775808";
    int flag = 0;
    for (uint32_t i = 0; i < str.size() && std::isdigit(str[i]); i++) {
      if (str[i] < MAX[i])
        flag = 1;
      else if (str[i] > MAX[i]) {
        if (flag == 0) {
          throw std::out_of_range("");
        }
      }
    }
    // <= +9223372036854775807
    if (flag == 0 && sign == 1) {
      throw std::out_of_range("");
    }
  }
  uint32_t i = 0;
  if (sign == 1) {
    for (; i < str.size() && std::isdigit(str[i]); i++) {
      ret = ret * 10 + (str[i] - '0');
    }
  } else {
    for (; i < str.size() && std::isdigit(str[i]); i++) {
      ret = ret * 10 - (str[i] - '0');
    }
  }
  if (i != str.size()) {
    throw std::invalid_argument("");
  }
  return ret;
}

/**
 * A simple tokenizer.
 * It uses trie to process reserved words and operators.
 * For numbers including integers and real numbers, strings, table names,
 * for simplicity it uses some hand-crafted matching functions.
 */

class SimpleTokenizer {
 public:
  SimpleTokenizer() {
    for (uint32_t i = 0; i < sizeof(all_tokens) / sizeof(char*); i++)
      all_tokens_and_op_trie.Insert(all_tokens[i], i);
  }
  void Init(std::string_view str) {
    str_ = str;
    index_ = 0;
    current_result_ = {0, _read()};
  }
  void Next() {
    if (current_result_.second.first != TokenType::_NONE) {
      current_result_ = {index_, _read()};
    }
  }
  // void Prev() { result_index_ -= 1; }
  std::pair<TokenType, std::string_view> Read() {
    return current_result_.second;
  }
  TokenType ReadType() { return Read().first; }
  std::string_view ReadStr() { return Read().second; }
  std::pair<uint32_t, std::string> CurrentPosition() {
    return {current_result_.first, _get_word(current_result_.second)};
  }

 private:
  // names of tables or columns are like (a-zA-z0-9_)+
  // Or "XXXX"
  std::pair<TokenType, std::string_view> match_name() {
    auto new_index = index_;
    int flag = 0;
    if (new_index < str_.size() && str_[new_index] == '\"') {
      flag = 1;
      new_index++;
    }
    while (new_index < str_.size() &&
           (std::isalpha(str_[new_index]) || std::isdigit(str_[new_index]) ||
               str_[new_index] == '_' || (flag && str_[new_index] != '\"'))) {
      new_index++;
    }
    if (new_index < str_.size() && str_[new_index] == '\"') {
      new_index++;
    } else if (flag) {
      return {TokenType::_TABLENAME_INVALID, {}};
    }
    auto old_index = index_;
    index_ = new_index;
    return {TokenType::_TABLENAME,
        str_.substr(old_index + flag, new_index - old_index - flag * 2)};
  }
  // Constants are like A or A.B or AeB or Ae-B or A. where A, B are non-empty
  // digit strings, i.e. (0-9)+
  std::pair<TokenType, std::string_view> match_constant() {
    if (str_[index_] == '.' &&
        (index_ >= str_.length() || !std::isdigit(str_[index_])))
      return {TokenType::_LITERIAL_INVALID_NUMBER, {}};
    auto new_index = index_;
    bool dots = false, es = false;
    while (new_index < str_.size() &&
           (std::isdigit(str_[new_index]) || str_[new_index] == '.' ||
               str_[new_index] == 'e' || str_[new_index] == 'E')) {
      if ((es || dots) && str_[new_index] == '.')
        return {TokenType::_LITERIAL_INVALID_NUMBER, {}};
      dots |= str_[new_index] == '.';
      if (str_[new_index] == 'e' || str_[new_index] == 'E') {
        if (new_index + 1 < str_.size() && !es) {
          es = true;
          if (str_[new_index + 1] == '-' || str_[new_index + 1] == '+')
            new_index += 1;
        } else
          return {TokenType::_LITERIAL_INVALID_NUMBER, {}};
      }
      new_index++;
    }
    if (new_index < str_.size() && dots && str_[new_index] == '.')
      return {TokenType::_LITERIAL_INVALID_NUMBER, {}};
    if (new_index < str_.size() &&
        (std::isalpha(str_[new_index]) || str_[new_index] == '_'))
      return {TokenType::_LITERIAL_INVALID_NUMBER, {}};
    auto old_index = index_;
    index_ = new_index;
    auto this_num = str_.substr(old_index, new_index - old_index);
    if (dots || es) {
      try {
        StringToDouble(this_num);
        return {TokenType::_LITERIAL_FLOAT, this_num};
      } catch (std::invalid_argument const& ex) {
        return {TokenType::_LITERIAL_INVALID_NUMBER, this_num};
      } catch (std::out_of_range const& ex) {
        return {TokenType::_LITERIAL_FLOAT_OVERFLOW, this_num};
      }
    } else {
      try {
        StringToInt(this_num);
        return {TokenType::_LITERIAL_INTEGER, this_num};
      } catch (std::invalid_argument const& ex) {
        return {TokenType::_LITERIAL_INVALID_NUMBER, this_num};
      } catch (std::out_of_range const& ex) {
        return {TokenType::_LITERIAL_INTEGER_OVERFLOW, this_num};
      }
    }
  }
  // strings are like 'XXXX'
  // For convenience, when it reads '\"', it will recognize it as '\' and '"'.
  // It will only skip '"', but it will not delete '\'.
  std::pair<TokenType, std::string_view> match_string() {
    auto new_index = index_;
    new_index++;
    while (new_index < str_.size() && str_[new_index] != '\'')
      new_index += str_[new_index] == '\\' ? 2 : 1;
    if (new_index >= str_.size() || str_[new_index] != '\'')
      return {TokenType::_LITERIAL_INVALID_STRING, {}};
    new_index++;
    auto old_index = index_;
    index_ = new_index;
    return {TokenType::_LITERIAL_STRING,
        str_.substr(old_index + 1, new_index - old_index - 2)};
  }

  std::pair<TokenType, std::string_view> _read() {
    if (!_eliminate_space())
      return {TokenType::_END, {}};
    // if the SQL command uses a reserved word, then it cannot write a-zA-z_0-9
    // behind it.
    auto rt = all_tokens_and_op_trie.Search(
        str_.substr(index_, str_.length() - index_));
    if (rt != -1) {
      auto len = std::strlen(all_tokens[rt]);
      if (rt >= static_cast<int32_t>(TokenType::_STAR) &&
          !(rt <= static_cast<int32_t>(TokenType::_OR) &&
              rt >= static_cast<int32_t>(TokenType::_AND))) {
        index_ += len;
        if (rt >= static_cast<int32_t>(TokenType::_OPERATOR))
          return {TokenType::_OPERATOR, str_.substr(index_ - len, len)};
        return {static_cast<TokenType>(rt), str_.substr(index_ - len, len)};
      }
      if (len + index_ == str_.length() ||
          (!std::isdigit(str_[len + index_]) &&
              !std::isalpha(str_[len + index_]) && str_[len + index_] != '_')) {
        index_ += len;
        if (rt >= static_cast<int32_t>(TokenType::_AND))
          return {TokenType::_OPERATOR, str_.substr(index_ - len, len)};
        return {static_cast<TokenType>(rt), str_.substr(index_ - len, len)};
      }
    }

    if (str_[index_] == '.' || isdigit(str_[index_]))
      return match_constant();
    else if (str_[index_] == '\'')
      return match_string();
    else if (isalpha(str_[index_]) || str_[index_] == '_' ||
             str_[index_] == '\"')
      return match_name();
    return {TokenType::_NONE, {}};
  }

  std::string _get_word(const std::pair<TokenType, std::string_view>& data) {
    if (data.first < TokenType::_OPERATOR) {
      return all_tokens[static_cast<uint32_t>(data.first)];
    }
    return std::string(data.second);
  }

  bool _eliminate_space() {
    while (index_ < str_.size() && std::isspace(str_[index_]))
      index_++;
    return index_ < str_.size();
  }
  std::string_view str_;
  uint32_t index_;
  // The cost of vector push_back is very high. For inserting 1e6 tuples of 3
  // fields, it takes 0.2 seconds out of 0.7 seconds.
  // std::vector<std::pair<uint32_t, std::pair<TokenType, std::string_view>>>
  // result_;
  std::pair<uint32_t, std::pair<TokenType, std::string_view>> current_result_;
  // uint32_t result_index_;
  Trie<256, 128> all_tokens_and_op_trie;
};

// Use LL(1) Parser.
class Parser::Impl {
  class ParserException : public std::exception {
    std::string errmsg_;

   public:
    ParserException(std::string errmsg) : errmsg_(errmsg) {}
    const char* what() const noexcept { return errmsg_.c_str(); }
  };

 public:
  std::pair<std::unique_ptr<Statement>, std::string> Parse(
      std::string_view statement) {
    reader_.Init(statement);
    try {
      auto statement = parse();
      return {std::move(statement), ""};
    } catch (const ParserException& e) {
      auto data = reader_.CurrentPosition();
      return {nullptr,
          fmt::format("Syntax error at {}, near \'{}\', error message: {}",
              data.first, data.second, e.what())};
    } catch (...) {
      auto data = reader_.CurrentPosition();
      return {nullptr, fmt::format("Syntax error at {}, near \'{}\'",
                           data.first, data.second)};
    }
  }

 private:
  SimpleTokenizer reader_;

  std::unique_ptr<Statement> parse() {
    auto [type, str] = reader_.Read();
    if (type == TokenType::SELECT) {
      return select_clause();
    } else if (type == TokenType::UPDATE) {
      return update_clause();
    } else if (type == TokenType::DROP) {
      return drop_clause();
    } else if (type == TokenType::CREATE) {
      return create_clause();
    } else if (type == TokenType::INSERT) {
      return insert_clause();
    } else if (type == TokenType::DELETE) {
      return delete_clause();
    }
    throw ParserException(
        "Expect \'select\' or \'update\' or \'drop\' or \'create\' or "
        "\'insert\'");
  }

  std::unique_ptr<SelectStatement> select_clause() {
    auto ret = select_or_subquery_clause();
    if (reader_.ReadType() != TokenType::_SEMICOLON)
      throw ParserException("Expect \';\'");
    reader_.Next();
    return ret;
  }

  std::unique_ptr<SelectStatement> select_or_subquery_clause() {
    auto ret = std::make_unique<SelectStatement>();
    _match_token("select", TokenType::SELECT);
    ret->is_distinct_ = false;
    if (reader_.ReadType() == TokenType::DISTINCT) {
      reader_.Next();
      ret->is_distinct_ = true;
    }
    ret->result_column_ = _get_list_of<std::unique_ptr<ResultColumn>>(
        [this]() { return result_column_clause(); });

    if (reader_.ReadType() == TokenType::_SEMICOLON)
      return ret;
    else if (reader_.ReadType() == TokenType::FROM) {
      reader_.Next();
      ret->tables_ = _get_list_of<std::unique_ptr<TableRef>>(
          [this]() { return table_clause(); });
    }

    if (reader_.ReadType() == TokenType::_SEMICOLON)
      return ret;
    else if (reader_.ReadType() == TokenType::WHERE) {
      reader_.Next();
      ret->predicate_ = expr_clause();
    }

    if (reader_.ReadType() == TokenType::_SEMICOLON)
      return ret;
    else if (reader_.ReadType() == TokenType::GROUP) {
      reader_.Next();
      _match_token("by", TokenType::BY);
      ret->group_by_ = _get_list_of<std::unique_ptr<Expr>>(
          [this]() { return expr_clause(); });
      if (reader_.ReadType() == TokenType::HAVING) {
        reader_.Next();
        ret->having_ = expr_clause();
      }
    }

    if (reader_.ReadType() == TokenType::_SEMICOLON)
      return ret;
    else if (reader_.ReadType() == TokenType::ORDER) {
      reader_.Next();
      _match_token("by", TokenType::BY);
      ret->order_by_ = _get_list_of<std::unique_ptr<OrderByElement>>(
          [this]() { return order_by_clause(); });
    }

    if (reader_.ReadType() == TokenType::_SEMICOLON)
      return ret;
    else if (reader_.ReadType() == TokenType::LIMIT) {
      reader_.Next();
      ret->limit_count_ = expr_clause();
      if (reader_.ReadType() == TokenType::OFFSET) {
        reader_.Next();
        ret->limit_offset_ = expr_clause();
      }
    }
    return ret;
  }

  std::unique_ptr<CreateTableStatement> create_table_clause() {
    auto ret = std::make_unique<CreateTableStatement>();
    _match_token("table", TokenType::TABLE);
    ret->table_name_ = table_name_clause();
    ret->columns_ = column_description_clause();
    if (reader_.ReadType() != TokenType::_SEMICOLON)
      throw ParserException("Expect \';\'");
    reader_.Next();
    return ret;
  }

  std::unique_ptr<CreateIndexStatement> create_index_clause() {
    auto ret = std::make_unique<CreateIndexStatement>();
    _match_token("index", TokenType::INDEX);
    ret->index_name_ = table_name_clause();
    _match_token("on", TokenType::ON);
    ret->table_name_ = table_name_clause();
    _match_token("(", TokenType::_LEFTQ);
    ret->indexed_column_names_ =
        _get_list_of<std::string>([this]() { return table_name_clause(); });
    _match_token(")", TokenType::_RIGHTQ);
    return ret;
  }

  std::unique_ptr<Statement> create_clause() {
    _match_token("create", TokenType::CREATE);
    auto [type, str] = reader_.Read();
    if (type == TokenType::TABLE)
      return create_table_clause();
    else if (type == TokenType::INDEX)
      return create_index_clause();
    else
      throw ParserException("Expect \'index\' or \'table\'.");
  }

  std::unique_ptr<DropTableStatement> drop_table_clause() {
    auto ret = std::make_unique<DropTableStatement>();
    _match_token("table", TokenType::TABLE);
    ret->table_name_ = table_name_clause();
    if (reader_.ReadType() != TokenType::_SEMICOLON)
      throw ParserException("Expect \';\'");
    reader_.Next();
    return ret;
  }

  std::unique_ptr<DropIndexStatement> drop_index_clause() {
    auto ret = std::make_unique<DropIndexStatement>();
    _match_token("index", TokenType::INDEX);
    ret->index_name_ = table_name_clause();
    if (reader_.ReadType() != TokenType::_SEMICOLON)
      throw ParserException("Expect \';\'");
    reader_.Next();
    return ret;
  }

  std::unique_ptr<Statement> drop_clause() {
    _match_token("drop", TokenType::DROP);
    auto [type, str] = reader_.Read();
    if (type == TokenType::TABLE)
      return drop_table_clause();
    else if (type == TokenType::INDEX)
      return drop_index_clause();
    else
      throw ParserException("Expect \'index\' or \'table\'.");
  }

  // insert statement. Two types:
  // (1) insert into A values (), (), ...
  // (2) insert into A select * from B;
  std::unique_ptr<InsertStatement> insert_clause() {
    auto ret = std::make_unique<InsertStatement>();
    _match_token("insert", TokenType::INSERT);
    _match_token("into", TokenType::INTO);
    ret->table_name_ = table_name_clause();
    if (reader_.ReadType() == TokenType::SELECT) {
      auto table = std::make_unique<SubqueryTableRef>();
      table->ch_ = select_or_subquery_clause();
      ret->insert_data_ = std::move(table);
    } else if (reader_.ReadType() == TokenType::VALUES) {
      ret->insert_data_ = value_table_clause();
    } else {
      throw ParserException("Expect \'values\' or \'select\'");
    }
    if (reader_.ReadType() != TokenType::_SEMICOLON)
      throw ParserException("Expect \';\'");
    reader_.Next();
    return ret;
  }

  // update statement.
  // update A set A.a = A.a + 1 where A.a > 2
  // update statement can also have from clause.
  // update A set A.a = A.a + B.b from (select sum(C.c) as b, C.id from C group
  // by 2) as B where C.id = A.id From SQLite documentation: If the join between
  // the target table and the FROM clause results in multiple output rows for
  // the same target table row, then only one of those output rows is used for
  // updating the target table. The output row selected is arbitrary and might
  // change from one release of SQLite to the next, or from one run to the next.
  std::unique_ptr<UpdateStatement> update_clause() {
    auto ret = std::make_unique<UpdateStatement>();
    _match_token("update", TokenType::UPDATE);
    ret->table_name_ = table_name_clause();
    _match_token("set", TokenType::SET);
    ret->updates_ = _get_list_of<std::unique_ptr<ColumnUpdate>>(
        [this]() { return set_clause(); });
    if (reader_.ReadType() == TokenType::_SEMICOLON)
      return ret;
    else if (reader_.ReadType() == TokenType::FROM) {
      reader_.Next();
      ret->other_tables_ = _get_list_of<std::unique_ptr<TableRef>>(
          [this]() { return table_clause(); });
    }
    if (reader_.ReadType() == TokenType::_SEMICOLON)
      return ret;
    else if (reader_.ReadType() == TokenType::WHERE) {
      reader_.Next();
      ret->predicate_ = expr_clause();
    }
    if (reader_.ReadType() != TokenType::_SEMICOLON)
      throw ParserException("Expect \';\'");
    reader_.Next();
    return ret;
  }

  std::unique_ptr<DeleteStatement> delete_clause() {
    auto ret = std::make_unique<DeleteStatement>();
    _match_token("delete", TokenType::DELETE);
    _match_token("from", TokenType::FROM);
    ret->table_name_ = table_name_clause();
    if (reader_.ReadType() == TokenType::_SEMICOLON)
      return ret;
    else if (reader_.ReadType() == TokenType::WHERE) {
      reader_.Next();
      ret->predicate_ = expr_clause();
    }
    if (reader_.ReadType() != TokenType::_SEMICOLON)
      throw ParserException("Expect \';\'");
    reader_.Next();
    return ret;
  }

  std::unique_ptr<ResultColumn> result_column_clause() {
    // ResultColumn := Column, Column...
    auto [type, str] = reader_.Read();
    if (type == TokenType::_STAR) {
      reader_.Next();
      auto ret = std::make_unique<StarResultColumn>();
      return ret;
    } else {
      auto ret = std::make_unique<ExprResultColumn>();
      ret->expr_ = expr_clause();
      if (reader_.ReadType() == TokenType::AS) {
        reader_.Next();
        ret->as_ = table_name_clause();
      }
      return ret;
    }
  }

  std::unique_ptr<OrderByElement> order_by_clause() {
    auto ret = std::make_unique<OrderByElement>();
    ret->expr_ = expr_clause();
    auto [type, str] = reader_.Read();
    if (type != TokenType::ASC && type != TokenType::DESC)
      throw ParserException("Expect \'asc\' or \'desc\'.");
    reader_.Next();
    ret->is_asc_ = type == TokenType::ASC;
    return ret;
  }

  // Match table name.
  std::string table_name_clause() {
    // table_name
    auto [type, str] = reader_.Read();
    // expect table name
    if (type == TokenType::_TABLENAME) {
      reader_.Next();
      return std::string(str);
    } else if (type == TokenType::_TABLENAME_INVALID) {
      throw ParserException("Invalid table name.");
    }
    throw ParserException("Error occurs when parsing table name.");
  }
  // Match AS clause.
  // It can be:
  // AS A
  // AS A(a, b, c)
  // It can also ignore AS.
  std::unique_ptr<TableAs> maybe_as_clause() {
    if (reader_.ReadType() == TokenType::_TABLENAME ||
        reader_.ReadType() == TokenType::AS) {
      if (reader_.ReadType() == TokenType::AS) {
        reader_.Next();
      }
      auto ret = std::make_unique<TableAs>();
      ret->table_name_ = table_name_clause();
      if (reader_.ReadType() != TokenType::_LEFTQ)
        return ret;
      _match_token("(", TokenType::_LEFTQ);
      ret->column_names_ =
          _get_list_of<std::string>([this]() { return table_name_clause(); });
      _match_token(")", TokenType::_RIGHTQ);
      return ret;
    } else {
      return nullptr;
    }
  }
  // Match tables in from clause.
  // For example,
  //    A
  //    A join B on A.a = B.a
  //    (select * from A join B) as A0 join A1 join (select * from C) as A2
  std::unique_ptr<TableRef> table_clause_with_level(uint32_t level) {
    auto [type, str] = reader_.Read();
    if (level == 0) {
      if (type == TokenType::_LEFTQ) {
        reader_.Next();
        // Subqueries must be in brackets.
        if (reader_.ReadType() == TokenType::SELECT) {
          auto ret = std::make_unique<SubqueryTableRef>();
          ret->ch_ = select_or_subquery_clause();
          _match_token(")", TokenType::_RIGHTQ);
          ret->as_ = maybe_as_clause();
          return ret;
        }
        // Values must be in brackets.
        if (reader_.ReadType() == TokenType::VALUES) {
          auto ret = value_table_clause();
          _match_token(")", TokenType::_RIGHTQ);
          ret->as_ = maybe_as_clause();
          return ret;
        }
        auto ret = table_clause();
        _match_token(")", TokenType::_RIGHTQ);
        ret->as_ = maybe_as_clause();
        return ret;
      } else if (type == TokenType::_TABLENAME) {
        // expect table name
        auto ret = std::make_unique<NormalTableRef>();
        ret->table_name_ = str;
        reader_.Next();
        ret->as_ = maybe_as_clause();
        return ret;
      } else if (type == TokenType::_TABLENAME_INVALID) {
        throw ParserException("Invalid table name.");
      } else {
        throw ParserException("Error occurs when parsing table clause.");
      }
    }

    auto left = table_clause_with_level(level - 1);
    // This is A join B on condition(A, B) join C on condition(AB, C) join D on
    // condition (ABC, D)... where A, B, C, D are tables or subqueries or
    // bracketed expressions. subqueries need to have alias.
    while (true) {
      auto [type, str] = reader_.Read();
      if (type != TokenType::JOIN && type != TokenType::INNER)
        break;
      reader_.Next();
      if (type == TokenType::INNER)
        _match_token("join", TokenType::JOIN);
      auto real_ret = std::make_unique<JoinTableRef>();
      real_ret->ch_[0] = std::move(left);
      real_ret->ch_[1] = table_clause_with_level(level - 1);
      if (reader_.ReadType() == TokenType::ON) {
        reader_.Next();
        real_ret->predicate_ = expr_clause();
      }
      left = std::move(real_ret);
    }
    return left;
  }

  std::unique_ptr<TableRef> table_clause() {
    return table_clause_with_level(1);
  }

  std::vector<ColumnDescription> column_description_clause() {
    std::vector<ColumnDescription> ret;
    _match_token("(", TokenType::_LEFTQ);
    // Table must have only one primary key.
    bool has_primary_key = false;
    // Table must have only one auto_increment key.
    // And this key must be the primary key.
    bool has_auto_inc = false;
    while (true) {
      auto [type, str] = reader_.Read();
      // expect column name
      if (type == TokenType::_TABLENAME) {
        reader_.Next();
        ColumnDescription col;
        col.column_name_ = std::string(str);
        auto [type, str] = reader_.Read();
        reader_.Next();
        // expect type
        if (type == TokenType::INT32) {
          col.types_ = FieldType::INT32;
          col.size_ = 4;
        } else if (type == TokenType::INTEGER || type == TokenType::INT64) {
          col.types_ = FieldType::INT64;
          col.size_ = 8;
        } else if (type == TokenType::CHAR) {
          col.types_ = FieldType::CHAR;
          _match_token("(", TokenType::_LEFTQ);
          auto [type, str] = reader_.Read();
          _deal_with_number_exception(type);
          reader_.Next();
          auto size = StringToInt(str);
          if (size > 256 || size < 1) {
            throw ParserException("Length of CHAR should be in [0, 256]");
          }
          col.size_ = size;
          _match_token(")", TokenType::_RIGHTQ);
        } else if (type == TokenType::FLOAT64 || type == TokenType::REAL) {
          col.types_ = FieldType::FLOAT64;
          col.size_ = 8;
        } else if (type == TokenType::VARCHAR) {
          col.types_ = FieldType::VARCHAR;
          _match_token("(", TokenType::_LEFTQ);
          auto [type, str] = reader_.Read();
          _deal_with_number_exception(type);
          reader_.Next();
          auto size = StringToInt(str);
          if (size > 256 || size < 1) {
            throw ParserException("Length of VARCHAR should be in [0, 256]");
          }
          col.size_ = size;
          _match_token(")", TokenType::_RIGHTQ);
        } else {
          throw ParserException("Invalid column type.");
        }
        // If it is auto increment.
        // Here we use MySQL syntax.
        if (reader_.ReadType() == TokenType::AUTOINC) {
          reader_.Next();
          if (has_auto_inc) {
            throw ParserException(
                "Table must have at most one auto_increment key.");
          } else {
            has_auto_inc = true;
          }
          if (col.types_ != FieldType::INT32 &&
              col.types_ != FieldType::INT64) {
            throw ParserException("Auto increment key must be integer.");
          }
          col.is_auto_gen_ = true;
        }
        // If it is primary key or foreign key.
        if (reader_.ReadType() == TokenType::PRIMARY) {
          reader_.Next();
          _match_token("key", TokenType::KEY);
          if (has_primary_key) {
            throw ParserException(
                "A table cannot have two or more primary keys.");
          } else {
            has_primary_key = true;
          }
          col.is_primary_key_ = true;
        }
        // Key can be both primary key and foreign key.
        if (reader_.ReadType() == TokenType::FOREIGN) {
          reader_.Next();
          _match_token("key", TokenType::KEY);
          _match_token("references", TokenType::REFERENCES);
          col.is_foreign_key_ = true;
          col.ref_table_name_ = table_name_clause();
          _match_token("(", TokenType::_LEFTQ);
          col.ref_column_name_ = table_name_clause();
          _match_token(")", TokenType::_RIGHTQ);
        }
        if (col.is_auto_gen_ && !col.is_primary_key_) {
          throw ParserException("Auto_increment field must be primary key.");
        }
        if (col.is_auto_gen_ && col.is_foreign_key_) {
          throw ParserException("Auto_increment field cannot be foreign key.");
        }
        ret.push_back(std::move(col));
        // Next column description
        if (reader_.ReadType() != TokenType::_COMMA)
          break;
        reader_.Next();
        continue;
      }
      throw ParserException("Invalid column description.");
    }
    _match_token(")", TokenType::_RIGHTQ);
    // Table cannot have duplicate column names.
    for (uint32_t i = 0; i < ret.size(); i++) {
      for (uint32_t j = 0; j < i; ++j)
        if (ret[i].column_name_ == ret[j].column_name_) {
          throw ParserException(fmt::format(
              "Duplicate column name \'{}\'.", ret[i].column_name_));
        }
    }
    return ret;
  }

  std::unique_ptr<ValuesTableRef> value_table_clause() {
    auto ret = std::make_unique<ValuesTableRef>();
    bool first_flag = true;
    _match_token("values", TokenType::VALUES);
    // auto start = std::chrono::system_clock::now();
    // TODO: std::unique_ptr<LiteralStringExpr> takes too much memory.
    // 1e7 std::unique_ptr<LiteralStringExpr>. Memory usage: 800M vs 300M.
    // Maybe we have to store the results of expressions.
    // StaticFieldVector vec;
    // std::vector<Field> vecs;
    while (true) {
      _match_token("(", TokenType::_LEFTQ);
      auto values = _get_list_of<Field>([this]() {
        auto expr = expr_clause();
        if (expr->type_ == ExprType::LITERAL_FLOAT) {
          return Field::CreateFloat(FieldType::FLOAT64, 8,
              static_cast<const LiteralFloatExpr*>(expr.get())->literal_value_);
        } else if (expr->type_ == ExprType::LITERAL_INTEGER) {
          return Field::CreateInt(FieldType::INT64, 8,
              static_cast<const LiteralIntegerExpr*>(expr.get())
                  ->literal_value_);
        } else if (expr->type_ == ExprType::LITERAL_STRING) {
          return Field::CreateString(
              FieldType::CHAR, static_cast<const LiteralStringExpr*>(expr.get())
                                   ->literal_value_);
        } else {
          throw ParserException(
              "Expression in the VALUES clause is not constant.");
        }
      });
      if (first_flag) {
        ret->num_fields_per_tuple_ = values.size();
        first_flag = false;
      } else if (ret->num_fields_per_tuple_ != values.size()) {
        throw ParserException("Different number of fields in VALUES table.");
      }
      ret->values_.insert(ret->values_.end(),
          std::make_move_iterator(values.begin()),
          std::make_move_iterator(values.end()));
      // for (auto& a : values) vec.Append(a);
      // for(auto& a : values)vecs.push_back(a);
      _match_token(")", TokenType::_RIGHTQ);
      if (reader_.ReadType() != TokenType::_COMMA) {
        break;
      }
      reader_.Next();
    }
    // auto end = std::chrono::system_clock::now();
    // std::chrono::duration<double> diff = end - start;
    // DB_INFO("{} s", diff.count());
    return ret;
  }

  std::unique_ptr<ColumnUpdate> set_clause() {
    auto ret = std::make_unique<ColumnUpdate>();
    auto [type, str] = reader_.Read();
    // Table.Column = Expr
    if (type == TokenType::_TABLENAME) {
      reader_.Next();
      auto [type2, str2] = reader_.Read();
      // if it only has column name
      // otherwise this is a dot
      if (type2 != TokenType::_DOT) {
        ret->table_name_ = "";
        ret->column_name_ = std::string(str);
        // expect '='
        if (type2 != TokenType::_OPERATOR || str2 != "=")
          throw ParserException("Expect \'=\'.");
        reader_.Next();
        ret->update_value_ = expr_clause();
        return ret;
      }
      reader_.Next();
      auto [type3, str3] = reader_.Read();
      // expect column_name
      if (type3 != TokenType::_TABLENAME)
        throw ParserException("Invalid column name.");
      reader_.Next();
      ret->table_name_ = std::string(str);
      ret->column_name_ = std::string(str3);
      auto [type4, str4] = reader_.Read();
      // expect '='
      if (type4 != TokenType::_OPERATOR || str4 != "=")
        throw ParserException("Expect \'=\'.");
      reader_.Next();
      ret->update_value_ = expr_clause();
      return ret;
    }
    throw ParserException("Expect column.");
  }

  bool is_literal(std::unique_ptr<Expr>& expr) {
    return expr->type_ == ExprType::LITERAL_FLOAT ||
           expr->type_ == ExprType::LITERAL_INTEGER ||
           expr->type_ == ExprType::LITERAL_STRING;
  }

  // If all operands are literal exprs then the expr can also be converted to
  // literal expr.
  std::unique_ptr<Expr> maybe_get_literal_expr(
      std::unique_ptr<Expr>& ch0, std::unique_ptr<Expr>& ch1, OpType op) {
    if (op == OpType::BITAND || op == OpType::BITLSH || op == OpType::BITRSH ||
        op == OpType::BITOR || op == OpType::BITXOR || op == OpType::MOD ||
        op == OpType::AND || op == OpType::OR) {
      if (ch0->type_ != ExprType::LITERAL_INTEGER ||
          ch1->type_ != ExprType::LITERAL_INTEGER) {
        throw ParserException(fmt::format(
            "Operator (OpType={}) only used between integers.", (uint32_t)op));
      }
    }
#define GEN_FUNC(new_expr_type, expect_op, op_a)        \
  if (op == OpType::expect_op) {                        \
    return std::make_unique<new_expr_type>(v0 op_a v1); \
  }
    if (ch0->type_ != ch1->type_) {
      if (ch0->type_ == ExprType::LITERAL_STRING) {
        throw ParserException(
            "Invalid operators between STRING and other type.");
      }
      double v0, v1;
      if (ch0->type_ == ExprType::LITERAL_FLOAT) {
        v0 = static_cast<LiteralFloatExpr*>(ch0.get())->literal_value_;
        v1 = static_cast<LiteralIntegerExpr*>(ch1.get())->literal_value_;
      } else {
        v0 = static_cast<LiteralIntegerExpr*>(ch0.get())->literal_value_;
        v1 = static_cast<LiteralFloatExpr*>(ch1.get())->literal_value_;
      }
      GEN_FUNC(LiteralIntegerExpr, LT, <);
      GEN_FUNC(LiteralIntegerExpr, GT, >);
      GEN_FUNC(LiteralIntegerExpr, LEQ, <=);
      GEN_FUNC(LiteralIntegerExpr, GEQ, >=);
      GEN_FUNC(LiteralIntegerExpr, EQ, ==);
      GEN_FUNC(LiteralIntegerExpr, NEQ, !=);
      GEN_FUNC(LiteralFloatExpr, ADD, +);
      GEN_FUNC(LiteralFloatExpr, MUL, *);
      GEN_FUNC(LiteralFloatExpr, SUB, -);
      GEN_FUNC(LiteralFloatExpr, DIV, /);
      throw ParserException("Invalid operator between FLOAT and INTEGER.");
    }
    if (ch0->type_ == ExprType::LITERAL_STRING) {
      auto v0 = static_cast<LiteralStringExpr*>(ch0.get())->literal_value_;
      auto v1 = static_cast<LiteralStringExpr*>(ch1.get())->literal_value_;
      GEN_FUNC(LiteralIntegerExpr, LT, <);
      GEN_FUNC(LiteralIntegerExpr, GT, >);
      GEN_FUNC(LiteralIntegerExpr, LEQ, <=);
      GEN_FUNC(LiteralIntegerExpr, GEQ, >=);
      GEN_FUNC(LiteralIntegerExpr, EQ, ==);
      GEN_FUNC(LiteralIntegerExpr, NEQ, !=);
      throw ParserException("Invalid operator between STRINGs.");
    }
    if (ch0->type_ == ExprType::LITERAL_FLOAT) {
      auto v0 = static_cast<LiteralFloatExpr*>(ch0.get())->literal_value_;
      auto v1 = static_cast<LiteralFloatExpr*>(ch1.get())->literal_value_;
      GEN_FUNC(LiteralIntegerExpr, LT, <);
      GEN_FUNC(LiteralIntegerExpr, GT, >);
      GEN_FUNC(LiteralIntegerExpr, LEQ, <=);
      GEN_FUNC(LiteralIntegerExpr, GEQ, >=);
      GEN_FUNC(LiteralIntegerExpr, EQ, ==);
      GEN_FUNC(LiteralIntegerExpr, NEQ, !=);
      GEN_FUNC(LiteralFloatExpr, ADD, +);
      GEN_FUNC(LiteralFloatExpr, MUL, *);
      GEN_FUNC(LiteralFloatExpr, SUB, -);
      GEN_FUNC(LiteralFloatExpr, DIV, /);
      throw ParserException("Invalid operator between FLOATs.");
    }
    if (ch0->type_ == ExprType::LITERAL_INTEGER) {
      auto v0 = static_cast<LiteralIntegerExpr*>(ch0.get())->literal_value_;
      auto v1 = static_cast<LiteralIntegerExpr*>(ch1.get())->literal_value_;
      GEN_FUNC(LiteralIntegerExpr, LT, <);
      GEN_FUNC(LiteralIntegerExpr, GT, >);
      GEN_FUNC(LiteralIntegerExpr, LEQ, <=);
      GEN_FUNC(LiteralIntegerExpr, GEQ, >=);
      GEN_FUNC(LiteralIntegerExpr, EQ, ==);
      GEN_FUNC(LiteralIntegerExpr, NEQ, !=);
      GEN_FUNC(LiteralIntegerExpr, ADD, +);
      GEN_FUNC(LiteralIntegerExpr, MUL, *);
      GEN_FUNC(LiteralIntegerExpr, SUB, -);
      GEN_FUNC(LiteralIntegerExpr, DIV, /);
      GEN_FUNC(LiteralIntegerExpr, BITAND, &);
      GEN_FUNC(LiteralIntegerExpr, BITOR, |);
      GEN_FUNC(LiteralIntegerExpr, BITXOR, ^);
      GEN_FUNC(LiteralIntegerExpr, BITLSH, <<);
      GEN_FUNC(LiteralIntegerExpr, BITRSH, >>);
      GEN_FUNC(LiteralIntegerExpr, MOD, %);
      GEN_FUNC(LiteralIntegerExpr, AND, &&);
      GEN_FUNC(LiteralIntegerExpr, OR, ||);
      throw ParserException("Invalid operator between INTEGERs.");
    }
    DB_ERR("Take impossible branch");
#undef GEN_FUNC
  }

  void maybe_constant_fold(std::unique_ptr<Expr>& expr) {
    if (expr->type_ == ExprType::BINCONDOP) {
      auto this_expr = static_cast<BinaryConditionExpr*>(expr.get());
      auto& ch0 = this_expr->ch0_;
      auto& ch1 = this_expr->ch1_;
      if (is_literal(ch0) && is_literal(ch1)) {
        expr = maybe_get_literal_expr(ch0, ch1, this_expr->op_);
      }
    } else if (expr->type_ == ExprType::BINOP) {
      auto this_expr = static_cast<BinaryExpr*>(expr.get());
      auto& ch0 = this_expr->ch0_;
      auto& ch1 = this_expr->ch1_;
      if (is_literal(ch0) && is_literal(ch1)) {
        expr = maybe_get_literal_expr(ch0, ch1, this_expr->op_);
      }
    } else if (expr->type_ == ExprType::UNARYCONDOP) {
      if (expr->ch0_->type_ == ExprType::LITERAL_STRING) {
        throw ParserException("Invalid operator on STRING.");
      } else if (expr->ch0_->type_ == ExprType::LITERAL_FLOAT) {
        throw ParserException("Invalid operator on FLOAT.");
      } else if (expr->ch0_->type_ == ExprType::LITERAL_INTEGER) {
        auto v0 =
            static_cast<LiteralIntegerExpr*>(expr->ch0_.get())->literal_value_;
        expr = std::make_unique<LiteralIntegerExpr>(!v0);
      }
    } else if (expr->type_ == ExprType::UNARYOP) {
      if (expr->ch0_->type_ == ExprType::LITERAL_STRING) {
        throw ParserException("Invalid operator on STRING.");
      } else if (expr->ch0_->type_ == ExprType::LITERAL_FLOAT) {
        auto v0 =
            static_cast<LiteralFloatExpr*>(expr->ch0_.get())->literal_value_;
        expr = std::make_unique<LiteralFloatExpr>(-v0);
      } else if (expr->ch0_->type_ == ExprType::LITERAL_INTEGER) {
        auto v0 =
            static_cast<LiteralIntegerExpr*>(expr->ch0_.get())->literal_value_;
        expr = std::make_unique<LiteralIntegerExpr>(-v0);
      }
    }
  }
  // F0 := (Expr) | Table.Column | "String" | Number | -F0 | not F0 | +F0 |
  // aggregate_function(Expr) F1 := F0 * F1 | F0 / F1 | F0 % F1 | F0 F2 := F1 +
  // F2 | F1 - F2 | F1 F3 := F2 ^ F3 | F2 & F3 | F2 | F3 | F2 << F3 | F2 >> F3
  // F4 := F3 > F4 | F3 < F4 | F3 <> F4 | F3 <= F4 | F3 >= F4 | F3 = F4 | F3
  // F5 := F4 and F5 | F4 or F5 | F4
  // Expr := F5

  // In C++, &, |, ^ have a lower level than comparison operators
  // But in SQL, comparison operators are lower.
  // https://learn.microsoft.com/en-us/sql/t-sql/language-elements/operator-precedence-transact-sql?view=sql-server-ver16
  std::pair<std::unique_ptr<Expr>, bool> expr_clause_with_level(
      uint32_t level) {
    if (level == 0) {
      auto [type, str] = reader_.Read();
      // F0 := Table.Column
      if (type == TokenType::_TABLENAME) {
        reader_.Next();
        auto [type2, str2] = reader_.Read();
        // column_name
        if (type2 != TokenType::_DOT) {
          auto ret = std::make_unique<ColumnExpr>("", std::string(str));
          return {std::move(ret), 0};
        }
        reader_.Next();
        auto [type3, str3] = reader_.Read();
        // expect name
        if (type3 != TokenType::_TABLENAME)
          throw ParserException("Invalid column name.");
        reader_.Next();
        // table_name.column_name
        auto ret =
            std::make_unique<ColumnExpr>(std::string(str), std::string(str3));
        return {std::move(ret), 0};
      } else if (type == TokenType::_LITERIAL_FLOAT) {
        // F0 := "String" | Number
        // std::stod is ugly and it can't work with std::string_view.
        // We have to use std::stod(std::string(str)).
        // And std::from_chars don't support double.
        // auto result = std::from_chars(str.data(), str.data() + str.size(),
        // number);

        // GCC's implementation for stod:
        // Wocao!!
        // inline double
        // stod(const string& __str, size_t* __idx = 0)
        // { return __gnu_cxx::__stoa(&std::strtod, "stod", __str.c_str(),
        // __idx); }
        auto ret = std::make_unique<LiteralFloatExpr>(StringToDouble(str));
        reader_.Next();
        return {std::move(ret), 0};
      } else if (type == TokenType::_LITERIAL_INTEGER) {
        // std::stoll is ugly too.
        auto ret = std::make_unique<LiteralIntegerExpr>(StringToInt(str));
        reader_.Next();
        return {std::move(ret), 0};
      } else if (type == TokenType::_LITERIAL_STRING) {
        auto ret = std::make_unique<LiteralStringExpr>(str);
        reader_.Next();
        return {std::move(ret), 0};
      } else if (type == TokenType::_LEFTQ) {
        // F0 := (Expr)
        reader_.Next();
        auto ret = expr_clause();
        maybe_constant_fold(ret);
        _match_token(")", TokenType::_RIGHTQ);
        return {std::move(ret), 1};
      } else if (type == TokenType::_OPERATOR &&
                 (str == "-" || str == "not" || str == "+")) {
        // F0 := -F0 | not F0 | +F0
        reader_.Next();
        // Check whether it is negative number
        if (str == "-") {
          auto [type, str] = reader_.Read();
          if (type >= TokenType::_LITERIAL_INTEGER &&
              type < TokenType::_LITERIAL_INVALID_NUMBER) {
            if (type == TokenType::_LITERIAL_INTEGER ||
                type == TokenType::_LITERIAL_INTEGER_OVERFLOW) {
              try {
                auto ret =
                    std::make_unique<LiteralIntegerExpr>(StringToInt(str, -1));
                reader_.Next();
                return {std::move(ret), 0};
              } catch (std::invalid_argument const& ex) {
                throw ParserException("Invalid number.");
              } catch (std::out_of_range const& ex) {
                throw ParserException("Integer number overflow.");
              }
            } else {
              try {
                auto ret =
                    std::make_unique<LiteralFloatExpr>(StringToDouble(str, -1));
                reader_.Next();
                return {std::move(ret), 0};
              } catch (std::invalid_argument const& ex) {
                throw ParserException("Invalid number.");
              } catch (std::out_of_range const& ex) {
                throw ParserException("Float number overflow.");
              }
            }
          }
        }
        auto [expr, _] = expr_clause_with_level(0);

        if (str == "+") {
          if (expr->type_ != ExprType::BINCONDOP &&
              expr->type_ != ExprType::UNARYCONDOP)
            return {std::move(expr), 0};
          else
            throw ParserException(
                "Invalid operand of type BOOLEAN to unary operator \'+\'.");
        } else if (str == "-") {
          if (expr->type_ != ExprType::BINCONDOP &&
              expr->type_ != ExprType::UNARYCONDOP)
            return {
                std::make_unique<UnaryExpr>(OpType::NEG, std::move(expr)), 0};
          else
            throw ParserException(
                "Invalid operand of type BOOLEAN to unary operator \'-\'.");
        } else {
          if (expr->type_ != ExprType::BINCONDOP &&
              expr->type_ != ExprType::UNARYCONDOP &&
              expr->type_ != ExprType::LITERAL_INTEGER)
            throw ParserException(
                "Operator NOT requires operands of type BOOLEAN.");
          return {std::make_unique<UnaryConditionExpr>(
                      OpType::NOT, std::move(expr)),
              0};
        }
      } else if (type == TokenType::MAX || type == TokenType::MIN ||
                 type == TokenType::SUM || type == TokenType::AVG ||
                 type == TokenType::COUNT) {
        // F0 := aggregate_function(Expr)
        reader_.Next();
        _match_token("(", TokenType::_LEFTQ);
        // count(*)
        if (type == TokenType::COUNT &&
            reader_.ReadType() == TokenType::_STAR) {
          reader_.Next();
          _match_token(")", TokenType::_RIGHTQ);
          return {std::make_unique<AggregateFunctionExpr>(std::string(str),
                      std::make_unique<LiteralIntegerExpr>(1)),
              0};
        }
        auto bracketed_expr = expr_clause();
        maybe_constant_fold(bracketed_expr);
        _match_token(")", TokenType::_RIGHTQ);
        return {std::make_unique<AggregateFunctionExpr>(
                    std::string(str), std::move(bracketed_expr)),
            0};
      } else {
        _deal_with_number_exception(type);
      }
      throw ParserException("Invalid expression.");
    }

    auto [left_expr, is_left_bracketed] = expr_clause_with_level(level - 1);
    maybe_constant_fold(left_expr);

    // We want the traverse order to be the real calculating order.
    // For convenient, we assume that all operators are non-communicate.
    // Multiplication is not communicate with division. For example: 2 * 5 / 10,
    // if we first calculate 5 / 10, Then it turns to 0 since it is integer
    // division.
    while (true) {
      auto [type, str] = reader_.Read();
      if (type == TokenType::_OPERATOR || type == TokenType::_STAR) {
        bool flag = false;
        bool flag_cond = false;
        bool flag_logic_cond = false;
        OpType op_type = OpType::ADD;
        // Determine the type of operators op_type
        // flag means this is a valid operator. If it is not operator, then we
        // return expr directly. flag_cond means this is a condition operator.
        // flag_logic_cond menas this is a logical operator.
        if (level == 1) {
          if (type == TokenType::_STAR || str == "*" || str == "/" ||
              str == "%") {
            if (type == TokenType::_STAR || str == "*")
              op_type = OpType::MUL;
            else if (str == "/")
              op_type = OpType::DIV;
            else
              op_type = OpType::MOD;
            flag = true;
          }
        } else if (level == 2) {
          if (str == "+" || str == "-") {
            if (str == "+")
              op_type = OpType::ADD;
            else
              op_type = OpType::SUB;
            flag = true;
          }
        } else if (level == 3 && (str == "<<" || str == ">>")) {
          if (str == "<<")
            op_type = OpType::BITLSH;
          else
            op_type = OpType::BITRSH;
          flag = true;
        } else if (level == 4 && str == "&") {
          op_type = OpType::BITAND;
          flag = true;
        } else if (level == 5 && str == "^") {
          op_type = OpType::BITXOR;
          flag = true;
        } else if (level == 6 && str == "|") {
          op_type = OpType::BITOR;
          flag = true;
        } else if (level == 7) {
          if (str == ">" || str == "<" || str == "<=" || str == ">=") {
            if (str == ">")
              op_type = OpType::GT;
            else if (str == "<")
              op_type = OpType::LT;
            else if (str == "<=")
              op_type = OpType::LEQ;
            else if (str == ">=")
              op_type = OpType::GEQ;
            flag = true;
            flag_cond = true;
          }
        } else if (level == 8 && (str == "=" || str == "<>")) {
          if (str == "<>")
            op_type = OpType::NEQ;
          else
            op_type = OpType::EQ;
          flag = true;
          flag_cond = true;
        } else if (level == 9) {
          if (str == "and" || str == "or") {
            if (str == "and")
              op_type = OpType::AND;
            else
              op_type = OpType::OR;
            flag = true;
            flag_cond = true;
            flag_logic_cond = true;
          }
        }
        if (flag) {
          reader_.Next();
          auto [right_expr, is_right_bracketed] =
              expr_clause_with_level(level - 1);
          // Logical operators only accept operands that return boolean, i.e.
          // expressions of condition operators.
          if ((left_expr->type_ == ExprType::BINCONDOP ||
                  left_expr->type_ == ExprType::UNARYCONDOP ||
                  left_expr->type_ == ExprType::LITERAL_INTEGER) &&
              (right_expr->type_ == ExprType::BINCONDOP ||
                  right_expr->type_ == ExprType::UNARYCONDOP ||
                  right_expr->type_ == ExprType::LITERAL_INTEGER) &&
              flag_logic_cond) {
            left_expr = std::make_unique<BinaryConditionExpr>(
                op_type, std::move(left_expr), std::move(right_expr));
            is_left_bracketed = 0;
          } else if (flag_logic_cond) {
            throw ParserException(fmt::format(
                "Invalid operand of type to binary operator \'{}\'.", str));
          } else if (flag_cond) {
            left_expr = std::make_unique<BinaryConditionExpr>(
                op_type, std::move(left_expr), std::move(right_expr));
            is_left_bracketed = 0;
          } else {
            left_expr = std::make_unique<BinaryExpr>(
                op_type, std::move(left_expr), std::move(right_expr));
            is_left_bracketed = 0;
          }
          maybe_constant_fold(left_expr);
        } else
          break;
      } else
        break;
    }

    // return the flag and the expr to higher level.
    return {std::move(left_expr), is_left_bracketed};
  }
  std::unique_ptr<Expr> expr_clause() {
    return expr_clause_with_level(9).first;
  }

  // If we have a function (func) to parse a clause T.
  // Then we can use this function to produce a function to parse clause T(,T)*.
  template <typename T, typename F>
  std::vector<T> _get_list_of(F&& func) {
    // This function gets T, T, ...
    std::vector<T> ret;
    while (true) {
      ret.push_back(func());
      auto [type, str] = reader_.Read();
      if (type != TokenType::_COMMA)
        break;
      reader_.Next();
    }
    return ret;
  }

  void _deal_with_number_exception(TokenType type) const {
    if (type == TokenType::_LITERIAL_FLOAT_OVERFLOW)
      throw ParserException("Float64 number overflow.");
    else if (type == TokenType::_LITERIAL_INTEGER_OVERFLOW)
      throw ParserException("Int64 number overflow.");
    else if (type == TokenType::_LITERIAL_INVALID_NUMBER)
      throw ParserException("Invalid number.");
    else if (type == TokenType::_LITERIAL_INVALID_STRING ||
             type == TokenType::_LITERIAL_STRING)
      throw ParserException("Expect number.");
  }

  // Try to match a token and move the tokenizer.
  void _match_token(std::string_view word, TokenType token) {
    if (reader_.ReadType() != token)
      throw ParserException(fmt::format("Expect \'{}\'.", word));
    reader_.Next();
  }
};

Parser::~Parser() {}

Parser::Parser() { ptr_ = std::make_unique<Parser::Impl>(); }

ParserResult Parser::Parse(std::string_view str, const DBSchema& schema) {
  auto [statement, errmsg] = ptr_->Parse(str);
  if (errmsg != "")
    return {std::move(errmsg)};

  if (statement->type_ == StatementType::CREATE_INDEX ||
      statement->type_ == StatementType::CREATE_TABLE ||
      statement->type_ == StatementType::DROP_INDEX ||
      statement->type_ == StatementType::DROP_TABLE) {
    return {std::move(statement), nullptr, ""};
  }

  BasicPlanGenerator planner(schema);
  auto [plan, errmsg2] = planner.Plan(statement.get());
  if (errmsg2 != "")
    return {std::move(errmsg2)};

  return {std::move(statement), std::move(plan), ""};
}

namespace impl {

template <typename T>
concept IsString = std::convertible_to<T, std::string>;

template <typename T>
concept CanPointerToString = requires(T v) {
  { v->ToString() }
  ->std::convertible_to<std::string>;
};
template <typename T>
concept HasStringMethod = requires(T v) {
  { v.ToString() }
  ->std::convertible_to<std::string>;
};

template <typename Ta, typename Tb, typename... T>
std::string SetToString(Ta&& first, Tb&& second,
    T&&... args) requires IsString<Ta>&& fmt::is_formattable<Tb>::value;
template <typename Ta, typename Tb, typename... T>
std::string SetToString(Ta&& first, const std::vector<Tb>& second,
    T&&... args) requires IsString<Ta>&& CanPointerToString<Tb>;

template <typename Ta, typename Tb, typename... T>
std::string SetToString(Ta&& first, const std::vector<Tb>& second,
    T&&... args) requires IsString<Ta>&& HasStringMethod<Tb>;
template <typename Ta, typename Tb, typename... T>
std::string SetToString(Ta&& first, const std::vector<Tb>& second,
    T&&... args) requires IsString<Ta>&& fmt::is_formattable<Tb>::value;
template <typename Ta, typename Tb, typename... T>
std::string SetToString(Ta&& first, const std::unique_ptr<Tb>& second,
    T&&... args) requires IsString<Ta>;

template <typename Ta, typename Tb, typename... T>
std::string SetToString(Ta&& first, Tb&& second,
    T&&... args) requires IsString<Ta>&& fmt::is_formattable<Tb>::value {
  auto func = [](auto&& first, auto&& second, bool is_last) {
    return is_last ? fmt::format("{}: {}", first, second)
                   : fmt::format("{}: {}, ", first, second);
  };
  if constexpr (sizeof...(args) > 0)
    return func(std::forward<Ta>(first), std::forward<Tb>(second), false) +
           impl::SetToString(std::forward<T>(args)...);
  else
    return func(std::forward<Ta>(first), std::forward<Tb>(second), true);
}

template <typename Ta, typename Tb, typename... T>
std::string SetToString(Ta&& first, const std::vector<Tb>& second,
    T&&... args) requires IsString<Ta>&& CanPointerToString<Tb> {
  std::string ret = fmt::format("{}: {{ ", first);
  for (uint32_t i = 0; i < second.size(); i++)
    ret += fmt::format(
        "{}{}", second[i]->ToString(), i + 1 < second.size() ? ", " : "");
  ret += " }";
  if constexpr (sizeof...(args) > 0)
    return ret + ", " + impl::SetToString(std::forward<T>(args)...);
  else
    return ret;
}

template <typename Ta, typename Tb, typename... T>
std::string SetToString(Ta&& first, const std::vector<Tb>& second,
    T&&... args) requires IsString<Ta>&& HasStringMethod<Tb> {
  std::string ret = fmt::format("{}: {{ ", first);
  for (uint32_t i = 0; i < second.size(); i++)
    ret += fmt::format(
        "{}{}", second[i].ToString(), i + 1 < second.size() ? ", " : "");
  ret += " }";
  if constexpr (sizeof...(args) > 0)
    return ret + ", " + impl::SetToString(std::forward<T>(args)...);
  else
    return ret;
}

template <typename Ta, typename Tb, typename... T>
std::string SetToString(Ta&& first, const std::vector<Tb>& second,
    T&&... args) requires IsString<Ta>&& fmt::is_formattable<Tb>::value {
  std::string ret = fmt::format("{}: {{ ", first);
  for (uint32_t i = 0; i < second.size(); i++)
    ret += fmt::format("{}{}", second[i], i + 1 < second.size() ? ", " : "");
  ret += " }";
  if constexpr (sizeof...(args) > 0)
    return ret + ", " + impl::SetToString(std::forward<T>(args)...);
  else
    return ret;
}

template <typename Ta, typename Tb, typename... T>
std::string SetToString(Ta&& first, const std::unique_ptr<Tb>& second,
    T&&... args) requires IsString<Ta> {
  return impl::SetToString(std::forward<Ta>(first),
      second == nullptr ? "" : second->ToString(), std::forward<T>(args)...);
}

}  // namespace impl

template <typename... T>
std::string SetToString(T&&... args) {
  return "{ " + impl::SetToString(std::forward<T>(args)...) + " }";
}

#define ref_vec(T, unique_ptr_vec) \
  ({                               \
    std::vector<T*> ret;           \
    for (auto& a : unique_ptr_vec) \
      ret.push_back(a.get());      \
    ret;                           \
  })

static const char* op_str[] = {"+", "-", "*", "/", "%", "&", "^", "|", "<<",
    ">>", "<", ">", "<=", ">=", "=", "<>", "and", "or", "not", "-"};

static const char* return_type_str[] = {"int", "float", "string", "null"};

static const char* field_type_str[] = {
    "int32", "int64", "float64", "char", "varchar", "empty"};

std::string BinaryExpr::ToString() const {
  return fmt::format("({}){}({})", ch0_->ToString(),
      op_str[static_cast<uint32_t>(op_)], ch1_->ToString());
}

std::string BinaryConditionExpr::ToString() const {
  return fmt::format("({}){}({})", ch0_->ToString(),
      op_str[static_cast<uint32_t>(op_)], ch1_->ToString());
}

std::string UnaryExpr::ToString() const {
  return fmt::format(
      "{}({})", op_str[static_cast<uint32_t>(op_)], ch0_->ToString());
}

std::string UnaryConditionExpr::ToString() const {
  return fmt::format(
      "{}({})", op_str[static_cast<uint32_t>(op_)], ch0_->ToString());
}

std::string LiteralStringExpr::ToString() const {
  return fmt::format("\"{}\"%string", literal_value_);
}

std::string LiteralIntegerExpr::ToString() const {
  return fmt::format("{}%int", literal_value_);
}

std::string LiteralFloatExpr::ToString() const {
  return fmt::format("{}%float", literal_value_);
}

std::string ColumnExpr::ToString() const {
  if (table_name_ == "") {
    return fmt::format("{}%{}%{}", column_name_, id_in_column_name_table_,
        return_type_str[static_cast<uint32_t>(ret_type_)]);
  }
  return fmt::format("{}.{}%{}%{}", table_name_, column_name_,
      id_in_column_name_table_,
      return_type_str[static_cast<uint32_t>(ret_type_)]);
}

std::string CastExpr::ToString() const {
  return fmt::format("{}({})",
      return_type_str[static_cast<uint32_t>(ret_type_)], ch0_->ToString());
}

std::string AggregateFunctionExpr::ToString() const {
  return fmt::format("{}({})", func_name_, ch0_->ToString());
}

std::string ExprResultColumn::ToString() const {
  return SetToString("expr result column", expr_, "as", as_);
}

std::string StarResultColumn::ToString() const {
  return "{ star result column }";
}

std::string OrderByElement::ToString() const {
  return SetToString("order by", expr_, "is asc", is_asc_);
}

std::string TableAs::ToString() const {
  return SetToString("table name", table_name_, "column names", column_names_);
}

std::string NormalTableRef::ToString() const {
  return SetToString("normal tableref", table_name_, "alias", as_);
}

std::string JoinTableRef::ToString() const {
  return SetToString("join tableref 0", ch_[0], "join table 1", ch_[1],
      "predicate", predicate_, "as", as_);
}

std::string SubqueryTableRef::ToString() const {
  return SetToString("subquery tableref", ch_, "as", as_);
}

std::string SelectStatement::ToString() const {
  return "select: " +
         SetToString("tables", ref_vec(TableRef, tables_), "result_column",
             ref_vec(ResultColumn, result_column_), "where", predicate_,
             "group by", ref_vec(Expr, group_by_), "having", having_,
             "order by", ref_vec(OrderByElement, order_by_), "limit count",
             limit_count_, "distinct", is_distinct_);
}

std::string ColumnDescription::ToString() const {
  return SetToString(
      "column_name", column_name_, "field_type", static_cast<uint32_t>(types_));
}

std::string CreateTableStatement::ToString() const {
  return "create table: " + SetToString("table name", table_name_,
                                "column descriptions", columns_);
}

std::string ColumnUpdate::ToString() const {
  return SetToString("table_name", table_name_, "column_name", column_name_,
      "update_value", update_value_);
}

std::string UpdateStatement::ToString() const {
  return "update: " + SetToString("table name", table_name_, "column updates",
                          ref_vec(ColumnUpdate, updates_), "predicate",
                          predicate_);
}

std::string InsertStatement::ToString() const {
  return "insert: " +
         SetToString("table name", table_name_, "insert data", insert_data_);
}

std::string DropTableStatement::ToString() const {
  return "drop table: " + SetToString("table name", table_name_);
}

std::string ValuesTableRef::ToString() const {
  return SetToString("values", values_, "number of fields per tuple",
      num_fields_per_tuple_, "as", as_);
}

std::string CreateIndexStatement::ToString() const {
  return "create index: " +
         SetToString("table name", table_name_, "index name", index_name_);
}

std::string DropIndexStatement::ToString() const {
  return "drop index: " + SetToString("index name", index_name_);
}

std::string DeleteStatement::ToString() const {
  return "delete: " +
         SetToString("table name", table_name_, "predicate", predicate_);
}

#undef ref_vec

std::unique_ptr<Expr> BinaryExpr::clone() const {
  auto ret = std::make_unique<BinaryExpr>(op_, ch0_->clone(), ch1_->clone());
  ret->ret_type_ = ret_type_;
  return ret;
}

std::unique_ptr<Expr> BinaryConditionExpr::clone() const {
  auto ret =
      std::make_unique<BinaryConditionExpr>(op_, ch0_->clone(), ch1_->clone());
  ret->ret_type_ = ret_type_;
  return ret;
}

std::unique_ptr<Expr> UnaryExpr::clone() const {
  auto ret = std::make_unique<UnaryExpr>(op_, ch0_->clone());
  ret->ret_type_ = ret_type_;
  return ret;
}
std::unique_ptr<Expr> UnaryConditionExpr::clone() const {
  auto ret = std::make_unique<UnaryConditionExpr>(op_, ch0_->clone());
  ret->ret_type_ = ret_type_;
  return ret;
}
std::unique_ptr<Expr> CastExpr::clone() const {
  auto ret = std::make_unique<CastExpr>(ch0_->clone());
  ret->ret_type_ = ret_type_;
  return ret;
}

std::unique_ptr<Expr> ColumnExpr::clone() const {
  auto ret = std::make_unique<ColumnExpr>(table_name_, column_name_);
  ret->ret_type_ = ret_type_;
  ret->id_in_column_name_table_ = id_in_column_name_table_;
  ret->id_table_in_planner_ = id_table_in_planner_;
  return ret;
}

std::unique_ptr<Expr> LiteralStringExpr::clone() const {
  auto ret = std::make_unique<LiteralStringExpr>(literal_value_);
  ret->ret_type_ = ret_type_;
  return ret;
}

std::unique_ptr<Expr> LiteralIntegerExpr::clone() const {
  auto ret = std::make_unique<LiteralIntegerExpr>(literal_value_);
  ret->ret_type_ = ret_type_;
  return ret;
}

std::unique_ptr<Expr> LiteralFloatExpr::clone() const {
  auto ret = std::make_unique<LiteralFloatExpr>(literal_value_);
  ret->ret_type_ = ret_type_;
  return ret;
}

std::unique_ptr<Expr> AggregateFunctionExpr::clone() const {
  auto ret = std::make_unique<AggregateFunctionExpr>(func_name_, ch0_->clone());
  ret->ret_type_ = ret_type_;
  return ret;
}

}  // namespace wing

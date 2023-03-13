### Cmdline

---

可以在命令行使用Wing。用法：`./wing file_name`。

如果要开启JIT，使用`./wing file_name --use_jit`。JIT会在`select`的时候调用。

一些命令行命令：

`show table`：可以列出DB中所有table。

`explain sql_statement`：可以输出`sql_statement`的execution plan。

`exit`或`quit`：退出DB。

`analyze table_name`：统计一个table。

### Wing SQL

---


Wing实现了SQL语法的一个子集。现在它不支持的操作包括：Nested subqueries，Window aggregate，CTE，Left/Right/Full outer join，Union/Intersect，Alter/Truncate/Rename table，String like，Exists/Any/All，Table Constraints，Null value，Create/Drop Index。

#### Create table

可以使用`create table`创建表。column有`int32`、`int64`、`float64`、`char`、`varchar`五种类型。现在最好不要用`char`。语法如下：

```sql
CREATE TABLE table_name(
     column_name_1 data_type [auto_increment] [primary key] [foreign key references ref_table_name(ref_column_name)],
     column_name_2 data_type [auto_increment] [primary key] [foreign key references ref_table_name(ref_column_name)],
     ...
);
```
其中`auto_increment`表示系统在输入0的时候会自动从1开始生成primary key，必须是整数类型。`primary key`表示这是这个table的primary key，每个table只能有一个primary key。primary key可以是任何类型，但是必须保证插入的primary key无重复。`foreign key references ref_table_name(ref_column_name)`表示这是一个foreign key，它引用了`ref_table_name`的`ref_column_name`列。这一列必须是primary key。

比如创建一个包含用户信息的表和一个包含消息记录的表：

```sql
create table user(id int64 auto_increment primary key, name varchar(20), password varchar(20));
create table message(id int64 auto_increment primary key, sender_id int64 foreign key references user(id), receiver_id int64 foreign key references user(id), message varchar(256));
```

可以使用`"xxx"`作为table的名字。比如`create table "select" (a int64);`创建了一个叫select的table。这个字符串也可以为空（但一般没人这么干）。

#### Drop table

语法如下：

```sql
drop table table_name;
```

#### Insert

语法如下：

```sql
insert into table_name values(column1, column2, column3)...;
insert into table_name select...;
```

比如向刚才创建的`user`和`message`插入行：

```sql
insert into user values(0, 'rabbit', '123456');
insert into user values(0, 'lain', 'aaabbbccc'), (0, 'toko', '987654321');
insert into message values(0, 1, 2, 'Hello world.');
insert into message select 0, user.id, user.id, 'test' from user;
```

输出结果如下：
```shell
wing> select * from user;
Parsing completed in 0.000109906 seconds.
Generate executor in 0.000160651 seconds.
Execute in 4.0517e-05 seconds.
+----+--------+-----------+
|  id|    name|   password|
+----+--------+-----------+
|   1|  rabbit|     123456|
|   2|    lain|  aaabbbccc|
|   3|    toko|  987654321|
+----+--------+-----------+

wing> select * from message;
Parsing completed in 0.000114612 seconds.
Generate executor in 0.000184234 seconds.
Execute in 4.1932e-05 seconds.
+----+-----------+-------------+--------------+
|  id|  sender_id|  receiver_id|       message|
+----+-----------+-------------+--------------+
|   1|          1|            2|  Hello world.|
|   2|          1|            1|          test|
|   3|          2|            2|          test|
|   4|          3|            3|          test|
+----+-----------+-------------+--------------+
```

注意，如果你要插入float64的值，你必须明确地把小数点打出来，或用科学计数法，否则会被识别为整数，进而发生parsing错误。

#### Delete

语法如下：

```sql
delete from table_name where predicate;
```

必须保证`predicate`的返回值是一个int。

比如删除刚才插入的`'test'`信息：

```sql
delete from message where message = 'test';
```

输出如下：

```shell
wing> delete from message where message = 'test';
Parsing completed in 0.000128898 seconds.
Generate executor in 0.000304886 seconds.
Execute in 0.000221912 seconds.
+--------------+
|  deleted rows|
+--------------+
|             3|
+--------------+
1 rows in total.

wing> select * from message;
Parsing completed in 0.000143741 seconds.
Generate executor in 0.000194638 seconds.
Execute in 3.846e-05 seconds.
+----+-----------+-------------+--------------+
|  id|  sender_id|  receiver_id|       message|
+----+-----------+-------------+--------------+
|   1|          1|            2|  Hello world.|
+----+-----------+-------------+--------------+
1 rows in total.
```

#### Select

语法太复杂，查阅SQL语法罢。

有一些不一样的地方：

表达式计算默认最高精度，整数用int64，浮点数用float64。没有溢出检查。

table别名：直接在table名后面写别名，也可以写as然后加别名。如：

```sql
select * from A A0, B A1, C as A2;
```

可以用`table_name(column1_name, column2_name...)`的形式给table的每个列别名。如：

```sql
select c, b, a from user U(a, b, c);
```

可以直接用这种形式给values产生的table命名：

```sql
select a + b * c from (values(2, 3, 4), (5, 6, 7)) A(a, b, c);
```

Subqueries和values：必须加括号，但不限制是否有别名。

如：
```sql
select a * b from (select * from (values(3, 4)) A(a, b), (values(9, 10, 11)));
```

Join：用`table_name join table_name on predicate`的形式。由于我们只考虑inner join，所以这里可以只写join。

如：
```sql
select * from user join message on user.id = message.sender_id;
```

直接用这个也能解析成Join：

```sql
select * from user, message where user.id = message.sender_id;
```

Group by和Order by：不支持用数字直接指定哪一列。如果你写出如下语句：

```sql
select a, sum(b) from A group by 1 order by 2;
```

那么`group by`和`order by`会直接按照整数`1`和`2`进行Aggregate和Sort。相当于没用。

Aggregate：支持输出非aggregate function且group by里没有的expression（与SQLite一致）。相当于在每个Group里随便找了一个tuple来计算。比如

```sql
select name, max(id) as c from user;
```

可能的输出结果：

```shell
+--------+-----+
|    name|    c|
+--------+-----+
|  rabbit|    3|
+--------+-----+

+--------+-----+
|    name|    c|
+--------+-----+
|    lain|    3|
+--------+-----+

+--------+-----+
|    name|    c|
+--------+-----+
|    toko|    3|
+--------+-----+
```

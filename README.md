# SQL 实验

## 安装数据库软件

这里提供 sqlite3 和 MySQL 的安装指南。你也可以选择安装 PostgreSQL 等数据库发行版，并在作业中注明使用的方言

### sqlite3

#### Linux

```shell
# Arch系
sudo pacman -S --needed sqlite3
# Debian系
sudo apt install sqlite3
```

#### MacOS

对于 macOS 用户，在终端中输入 `sqlite3` 即可使用系统预装软件包

#### Windows

在[官网](https://www.sqlite.org/download.html)下载 `sqlite-tools-win` 软件包，然后添加到系统变量

### MySQL

#### 安装

##### Linux

使用包管理器安装，例如

```shell
sudo apt install mysql-server
sudo systemctl start mysql
```

##### MacOS

使用 brew 或在官网下载安装镜像

```bash
brew install mysql
brew services start mysql
```

##### Windows

推荐在 WSL2 下操作

```bash
sudo apt install mysql-server
sudo service mysql start
```

#### 设置密码

#### 连入

```bash
sudo mysql_secure_installation
sudo mysql -uroot -p
```

## 数据集 2pts

我们使用 TPC-H 数据集作为作业的数据来源

具体来说，我们提供了一些 csv 文件，你们需要首先将其导入数据库中

## 单表查询 5*1pts

对于以下需求，设计 SQL 语句输出结果，你只需要提交对应 SQL 语句

注意以下语句顺序执行，前序操作可能影响后续结果

1. （`ORDER`）求 `order` 数大于 20 的 `customer` 的 `O_TOTALPRICE`（求和，即每个 customer 返回一条记录）
2. （`LINEITEM`）对 `discount` 大于 0.02 的 `tax` 加 10%
3. （`LINEITEM`）对所有 `tax` 小于 0.05 的物品（`L_ORDERKEY`, `L_LINENUMBER`）按照 `L_ORDERKEY` 计算平均 `discount`
   1. 对结果按平均 `discount` 从大到小排序
   2. 展示平均 `discount` 最大的 10 行
4. （`LINEITEM`）求 `discount` 最大的 `item`，用 `L_ORDERKEY` 和 `L_LINENUMBER` 表示
   1. 禁止使用 `agg` 操作（即需要用基本运算符表示MAX的逻辑）
5. （`PARTSUPP`）对于相同的 `PS_PARTKEY`，求所有供应商的 `PS_AVAILQTY` 之和

## 多表查询 3*1pts

1. 解释如下 SQL 的作用

   ```sql
   SELECT `C_NAME`, `O_ORDERSTATUS`, `N_NATIONKEY` FROM `customer`, `order`, `nation` WHERE `C_CUSTKEY`=`O_CUSTKEY` AND `C_NATIONKEY`=`N_NATIONKEY` AND `N_NAME`='CHINA'
   ```

2. （`CUSTOMER`, `ORDER`）求所有 `total_price` 小于 10000 的 `customer` 行

3. 将 1 中表范围增加 `LINEITEM` 表，然后合理选择顺序使之执行最快

## 提交

我们期待一份纯文本格式提交，如 markdown 或 txt；二进制格式如 pdf 等也被允许，但请尽量附一份 txt 列出所用的 sql 语句，这是因为 pdf 的文本比较难用

对于作业若有问题，可以在群聊/网络学堂提问。

## 一些可能有用的链接

<https://www.runoob.com/sqlite/sqlite-tutorial.html>

<https://www.runoob.com/mysql/mysql-tutorial.html>

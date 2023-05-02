CREATE TABLE aka_name (
    id int32 auto_increment PRIMARY KEY,
    person_id int32,
    name varchar(255)
);

CREATE TABLE char_name (
    id int32 auto_increment PRIMARY KEY,
    name varchar(255)
);

CREATE TABLE cast_info (
    id int32 auto_increment PRIMARY KEY,
    person_id int32,
    movie_id int32,
    person_role_id int32,
    note varchar(255),
    role_id int32
);

CREATE TABLE company_name (
    id int32 auto_increment PRIMARY KEY,
    name varchar(255),
    country_code varchar(255)
);

CREATE TABLE info_type (
    id int32 auto_increment PRIMARY KEY,
    info varchar(32)
);

CREATE TABLE movie_info (
    id int32 auto_increment PRIMARY KEY,
    movie_id int32,
    info_type_id int32,
    info varchar(255),
    note varchar(255)
);

CREATE TABLE movie_companies (
    id int32 auto_increment PRIMARY KEY,
    movie_id int32,
    company_id int32,
    company_type_id int32,
    note varchar(255)
);

CREATE TABLE name (
    id int32 auto_increment PRIMARY KEY,
    name varchar(255),
    gender int32
);

CREATE TABLE role_type (
    id int32 auto_increment PRIMARY KEY,
    role varchar(32)
);

CREATE TABLE title (
    id int32 auto_increment PRIMARY KEY,
    title varchar(255),
    production_year int32
);

CREATE TABLE company_type (
    id int32 auto_increment PRIMARY KEY,
    kind varchar(32)
);

CREATE TABLE person_info (
    id int32 auto_increment PRIMARY KEY,
    person_id int32,
    info_type_id int32,
    info varchar(255),
    note varchar(255)
);
# Generate random SQL queries for demo
import btree_framework
from sqlParser import SQL


def generator():

    sql = "CREATE TABLE Rel_i_i_1000 ( c1 INT PRIMARY KEY, c2 INT );"
    btree_framework.operation(SQL(sql))
    sql = "INSERT INTO Rel_i_i_1000 (c1, c2) VALUES "
    for i in range(1, 1001):
        if i != 1:
            sql += ","
        sql += "({}, {})".format(i, i)
    btree_framework.operation(SQL(sql))

    sql = "CREATE TABLE Rel_i_i_10000 ( c1 INT PRIMARY KEY, c2 INT );"
    btree_framework.operation(SQL(sql))
    sql = "INSERT INTO Rel_i_i_10000 (c1, c2) VALUES "
    for i in range(1, 10001):
        if i != 1:
            sql += ","
        sql += "({}, {})".format(i, i)
    btree_framework.operation(SQL(sql))

    sql = "CREATE TABLE Rel_i_i_100000 ( c1 INT PRIMARY KEY, c2 INT );"
    btree_framework.operation(SQL(sql))
    sql = "INSERT INTO Rel_i_i_100000 (c1, c2) VALUES "
    for i in range(1, 100001):
        if i != 1:
            sql += ","
        sql += "({}, {})".format(i, i)
    btree_framework.operation(SQL(sql))

    sql = "CREATE TABLE Rel_i_1_1000 ( c1 INT PRIMARY KEY, c2 INT );"
    btree_framework.operation(SQL(sql))
    sql = "INSERT INTO Rel_i_1_1000 (c1, c2) VALUES "
    for i in range(1, 1001):
        if i != 1:
            sql += ","
        sql += "({}, {})".format(i, 1)
    btree_framework.operation(SQL(sql))

    sql = "CREATE TABLE Rel_i_1_10000 ( c1 INT PRIMARY KEY, c2 INT );"
    btree_framework.operation(SQL(sql))
    sql = "INSERT INTO Rel_i_1_10000 (c1, c2) VALUES "
    for i in range(1, 10001):
        if i != 1:
            sql += ","
        sql += "({}, {})".format(i, 1)
    btree_framework.operation(SQL(sql))

    sql = "CREATE TABLE Rel_i_1_100000 ( c1 INT PRIMARY KEY, c2 INT );"
    btree_framework.operation(SQL(sql))
    sql = "INSERT INTO Rel_i_1_100000 (c1, c2) VALUES "
    for i in range(1, 100001):
        if i != 1:
            sql += ","
        sql += "({}, {})".format(i, 1)
    btree_framework.operation(SQL(sql))

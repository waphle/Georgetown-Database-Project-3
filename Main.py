from sqlParser import SQL
import btree_framework
import Redis_test
import parser_test
from redis import StrictRedis

db = StrictRedis(host='127.0.0.1', port=6379, db=0)
db.flushdb()
sql = "CREATE TABLE example_table (id INT, grade INT);"
btree_framework.operation(SQL(sql))
sql = "ALTER TABLE example_table ADD PRIMARY KEY (id);"
btree_framework.operation(SQL(sql))
sql = "INSERT INTO example_table (id, grade) VALUES (1, 10), (2, 20), (3, 15), (4, 5), (5, 30);"
btree_framework.operation(SQL(sql))
Redis_test.data_store(db, btree_framework.databases["example_table"], "example_table")
while True:
    sql = SQL(str(input("Input your sql command:\n")))
    if sql.operation.upper() == 'SELECT':
        databases = Redis_test.data_load(db, sql.table)
    btree_framework.operation(sql)

    if sql.operation.upper() == 'DROP':
        if db.exists(sql.table):
            db.delete(sql.table)

    if sql.operation.upper() == 'INSERT' or sql.operation.upper() == 'UPDATE' or sql.operation.upper() == 'DELETE':
        if db.exists(sql.table):
            db.delete(sql.table)
        Redis_test.data_store(db, btree_framework.databases[sql.table], sql.table)



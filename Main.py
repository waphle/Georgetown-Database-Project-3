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
while True:
    sql = str(input("Input your sql command:\n"))
    btree_framework.operation(SQL(sql))

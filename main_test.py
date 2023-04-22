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
# temp_tree = {}
# keys = list(btree_framework.databases["example_table"].tree.keys())
# print(keys)
# values = list(btree_framework.databases["example_table"].tree.values())
# print(values)
# for i in range(len(keys)):
#     temp_tree[keys[i]] = values[i]
Redis_test.data_store(db, btree_framework.databases["example_table"], "example_table")
t = Redis_test.data_load(db, 'example_table')
keys = list(t.tree.keys())
print(keys)
values = list(t.tree.values())
print(values)
print(t.tree.get('2'))
print(t.tree.get('3'))
print(t.tree.get('4'))

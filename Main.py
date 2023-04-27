from sqlParser import SQL
import btree_framework
import Redis_test
import parser_test
from redis import StrictRedis
import demo_sql
import time

db = StrictRedis(host='127.0.0.1', port=6379, db=0)
# db.flushdb()
# demo_sql.generator()
# Redis_test.data_store(db, btree_framework.databases["Rel_i_i_1000"], "Rel_i_i_1000")
# Redis_test.data_store(db, btree_framework.databases["Rel_i_i_10000"], "Rel_i_i_10000")
# Redis_test.data_store(db, btree_framework.databases["Rel_i_i_100000"], "Rel_i_i_100000")
# Redis_test.data_store(db, btree_framework.databases["Rel_i_1_1000"], "Rel_i_1_1000")
# Redis_test.data_store(db, btree_framework.databases["Rel_i_1_10000"], "Rel_i_1_10000")
# Redis_test.data_store(db, btree_framework.databases["Rel_i_1_100000"], "Rel_i_1_100000")
while True:
    sql = SQL(str(input("Input your sql command:\n")))
    start_time = time.time()
    if sql.operation.upper() == 'SELECT':
        btree_framework.databases = Redis_test.data_load(db, sql.table)
        btree_framework.operation(sql)

    elif sql.operation.upper() == 'DROP':
        if db.exists(sql.table):
            db.delete(sql.table)

    elif sql.operation.upper() == 'INSERT' or sql.operation.upper() == 'UPDATE' or sql.operation.upper() == 'DELETE':
        btree_framework.databases = Redis_test.data_load(db, sql.table)
        btree_framework.operation(sql)
        if db.exists(sql.table):
            db.delete(sql.table)
        Redis_test.data_store(db, btree_framework.databases[sql.table], sql.table)

    elif sql.operation.upper() == 'JOIN':
        btree_framework.databases = Redis_test.data_load(db, sql.table)
        btree_framework.databases[sql.joint] = Redis_test.data_load(db, sql.joint)[sql.joint]
        btree_framework.operation(sql)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")




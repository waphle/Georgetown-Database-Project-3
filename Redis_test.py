from redis import StrictRedis
import pandas as pd
import pickle
# import btree_framework as bt


def data_store(db, table):
    name_t = table.name
    keys = list(table.tree.keys())
    values = list(table.tree.values())
    for i in range(len(keys)):
        db.hset(name_t, keys[i], values[i])
    return db.hgetall(name_t)


def data_insert(db, name, tree):
    keys = list(tree.keys())
    values = list(tree.values())
    for i in range(len(keys)):
        db.hset(name, keys[i], values[i])
    return db.hgetall(name)


def data_del(db, name, keys):
    db.hdel(name, keys)
    return db.hgetall(name)


def table_del(db, name):
    return db.delete(name)


db = StrictRedis(host='127.0.0.1', port=6379, db=0)
# table = bt.Table()
# data_store(db, table)
t = {1: "red", 2: "green", 3: "blue", 4: "spades"}
keys = list(t.keys())
values = list(t.values())
for i in range(len(keys)):
    db.hset(name='test', key=keys[i], value=values[i])
print(db.hgetall('test'))
print(db.delete('test'))
print(db.get('test'))

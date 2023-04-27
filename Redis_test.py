from redis import StrictRedis
import json
from BTrees.OOBTree import OOBTree
import btree_framework
from sqlParser import SQL
from btree_framework import Table


def data_store(db, table, name):
    temp_tree = {}
    keys = list(table.tree.keys())
    values = list(table.tree.values())
    for i in range(len(keys)):
        temp_tree[keys[i]] = values[i]
    temp_dict = {'tree': temp_tree, 'attributes': table.attribute, 'type': table.type, 'PK': table.PK,
                 'FK': table.FK, 'FK_table': table.FK_table}
    json_data = json.dumps(temp_dict)
    db.set(name, json_data)
    return 'Data has been successfully stored.'


def data_load(db, name):
    retrieved_json_data = db.get(name)
    retrieved_data = json.loads(retrieved_json_data)
    d = Table(sql=None)
    d.tree.update(retrieved_data['tree'])
    d.attribute = retrieved_data['attributes']
    d.type = retrieved_data['type']
    d.PK = retrieved_data['PK']
    d.FK = retrieved_data['FK']
    d.FK_table = retrieved_data['FK_table']
    database = {name: d}
    return database


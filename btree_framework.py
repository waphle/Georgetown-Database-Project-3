from BTree import btree
from sqlParser import SQL

class Table:
    def __init__(self, name, PK) -> None:
        self.tree = btree.BTree(1)
        self.PK = PK
        self.name = name

    def __init__(self, name) -> None:
        self.tree = btree.BTree(1)
        self.name = name

    def insert_row(self, row):
        key = ""
        for k in self.PK:
            k += "(" + row[k] + ")"
        self.tree[key]= row

    def delete_row(self, key):
        del self.tree[key]

    def get_row(self, key):
        return self.tree.get(key)
    
def operation(sql):
    if sql.operation == "CREATE":
        test = sql # Code here
        
def insertion(btree, sqlParser, sql):
    btree = BTree()
    sql = "INSERT INTO my_table (key, value) VALUES (1, 'one')"
    insertion(btree, sql)
    
    query = sqlParser(sql)

    key = query['values'][0]
    value = query['values'][1]

    btree.set(key, value)
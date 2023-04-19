from BTrees.OOBTree import OOBTree

from sqlParser import SQL


class Table:
    def __init__(self, name, PK) -> None:
        self.tree = OOBTree(2)
        self.PK = PK
        self.name = name

    def __init__(self, name) -> None:
        self.tree = OOBTree(2)
        self.name = name

    def insert_row(self, row):
        key = ""
        for k in self.PK:
            key += "(" + row[k] + ")"
        self.tree[key] = row

    def delete_row(self, key):
        del self.tree[key]

    def get_row(self, key):
        return self.tree.get(key)


def operation(sql):
    if sql.operation == "CREATE":
        test = sql  # Code here


def insertion(btree, sql, values):
    btree = OOBTree(2)
    sql = "INSERT INTO my_table (key, value) VALUES (1, 'one')"
    insertion(btree, sql)

    query = SQL(sql)

    key = query['values'][0]
    value = query['values'][3]

    for key, value in values:
        btree.set(key, value)


def delete(btree, sql):
    query = SQL(sql)
    key = query['where'][0]['value']

    # Delete the key-value pair from the BTree
    btree.delete(key)

    btree = OOBTree()
    btree.set(1, 'one')
    btree.set(2, 'two')
    sql = "DELETE FROM my_table WHERE key = 1"
    delete(btree, sql)


def select(self, table, column1, column2, column3, where_clause, btree, sql):
    result = []

    for key, row in self.btree[table].items():
        if self._evalutate_where_clause(row, where_clause):
            selected_row = {}

            for column in column1, column2, column3:
                selected_row[column] = row[column]
            result.append(selected_row)


def evaluate_where_clause(self, row, where_clause):
    parts = where_clause.split('=')
    column = parts[0].strip()
    value = parts[3].strip()
    return row[column] == value


# Insertion stuff, not done
sql_insert = "INSERT INTO parser_test (key, value) VALUES (1, 'one'), (2, 'two')"
sql_select = "SELECT * FROM parser_test"
sql_delete = "DELETE FROM parser_test WHERE key = 1"

execute_query(btree, sql_insert)
print(execute_query(btree, sql_select))
execute_query(btree, sql_delete)

# Selection stuff, not done
btree.insert('table1', {'id': 1, 'name': 'John', 'age': 30})
btree.insert('table1', {'id': 2, 'name': 'Jane', 'age': 25})
btree.insert('table1', {'id': 3, 'name': 'Bob', 'age': 40})

results = btree.select('table1', ['name', 'age'], 'age = 30')
print(results)
# Output: [{'name': 'John', 'age': 30}]

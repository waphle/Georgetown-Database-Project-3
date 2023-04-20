from BTrees.OOBTree import OOBTree
from sqlParser import SQL

# list saved in main memory.
databases = {}


class Table:
    def __init__(self, sql) -> None:
        self.tree = OOBTree()
        self.attribute = sql.attributes
        self.type = sql.type
        self.PK = []
        if True in sql.PK:
            i = 0
            for pk in sql.PK:
                if pk:
                    self.PK.append(sql.attributes[i])
                i += 1
        if sql.FK:
            self.FK = sql.FK
        if sql.not_null:
            self.not_null = sql.not_null
        if sql.values:
            self.default = sql.values

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
        databases[sql.table] = Table(sql)
    elif sql.operation == "INSERT":
        if databases.get(sql.table) is not None:
            database = databases.get(sql.table)
            # check attributes
            for col in sql.attributes:
                if col not in database.attribute:
                    print("Error, wrong column!")
                    return
            for col in database.attribute:
                if col not in sql.attributes:
                    if database.not_null[database.attribute.index(col)]:
                        print("Error, not null column not given value!")
                        return
            if not database.PK:
                print("Error, define the primary key!")
            data = {}
            for col in database.attribute:
                if col in sql.attributes:
                    if database.type[database.attribute.index(col)] == "INT":
                        data[col] = int(sql.insert_values[sql.attributes.index(col)])
                    else:
                        data[col] = sql.insert_values[sql.attributes.index(col)]
                else:
                    if database.type[database.attribute.index(col)] == "INT":
                        data[col] = int(database.default[database.attribute.index(col)])
                    else:
                        data[col] = database.default[database.attribute.index(col)]
            if len(database.PK) == 1:
                if database.type[sql.attributes.index(database.PK[0])] == "INT":
                    database.tree.insert(int(data[database.PK[0]]), data)
                else:
                    database.tree.insert(data[database.PK[0]], data)
            else:
                key = ""
                for pk in database.PK:
                    key += ":" + data[pk]
                database.tree.insert(key, data)
            databases[sql.table] = database
        else:
            print("Table not exist!")
            return


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


# Aggregator operator
def node_min(node):
    return min(node)


def node_max(node):
    return max(node)


def node_sum(node):
    return sum(node)


def agg(node, func):
    BTree = OOBTree(3)
    if node.is_leaf():
        return func(node)
    else:
        return func(node) + sum([agg(child, func) for child in node.children()])

# # Insertion stuff, not done, probably not needed
# sql_insert = "INSERT INTO parser_test (key, value) VALUES (1, 'one'), (2, 'two')"
# sql_select = "SELECT * FROM parser_test"
# sql_delete = "DELETE FROM parser_test WHERE key = 1"

# execute_query(btree, sql_insert)
# print(execute_query(btree, sql_select))
# execute_query(btree, sql_delete)

# # Selection stuff, not done, probably not needed
# btree.insert('table1', {'id': 1, 'name': 'John', 'age': 30})
# btree.insert('table1', {'id': 2, 'name': 'Jane', 'age': 25})
# btree.insert('table1', {'id': 3, 'name': 'Bob', 'age': 40})

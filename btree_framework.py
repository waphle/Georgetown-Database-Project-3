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
        self.FK = []
        self.FK_table = []
        if True in sql.FK:
            self.FK = sql.FK
            self.FK_table = sql.FK_table
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
        if databases.get(sql.table) is not None:
            print("Error, table already exist!")
            return
        databases[sql.table] = Table(sql)
    elif sql.operation == "DROP":
        if databases.get(sql.table) is not None:
            del databases[sql.table]
        else:
            print("Error, no such table!")
            return
    elif sql.operation == "ALTER":
        if databases.get(sql.table) is not None:
            database = databases.get(sql.table)
            if sql.PK is not None:
                for pk in sql.PK:
                    if pk not in database.PK:
                        database.PK.append(pk)
            databases[sql.table] = database
        else:
            print("Error, no such table!")
            return
    elif sql.operation == "INSERT":
        if databases.get(sql.table) is not None:
            database = databases.get(sql.table)
        else:
            print("Table not exist!")
            return

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
                    data[col] = sql.insert_values[sql.attributes.index(col)][1:-1]
            else:
                if database.type[database.attribute.index(col)] == "INT":
                    data[col] = int(database.default[database.attribute.index(col)])
                else:
                    data[col] = database.default[database.attribute.index(col)][1:-1]
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
    elif sql.operation == "UPDATE":
        if databases.get(sql.table) is not None:
            database = databases.get(sql.table)
        else:
            print("Table not exist!")
            return
        result = OOBTree()
        if sql.conditions:
            f = 0
            for condition in sql.conditions:
                if condition != "AND" and condition != "OR":
                    if condition["column"] not in database.attribute:
                        print("Error, wrong column name!")
                    i = database.attribute.index(condition["column"])
                    new_condition = condition.copy()
                    if database.type[i] == "INT":
                        new_condition["value"] = int(condition["value"])
                    else:
                        new_condition["value"] = condition["value"][1:-1]
                if condition == "AND":
                    f = 1
                elif condition == "OR":
                    f = 0
                elif f == 0:
                    for pk, data in database.tree.items():
                        or_condition(result, data, pk, new_condition)
                elif f == 1:
                    new_result = result
                    for pk, data in result.items():
                        and_condition(new_result, data, pk, new_condition)
                    result = new_result
        else:
            result = database.tree
        for pk, data in result.items():
            new_data = data.copy()
            for attribute in sql.attributes:
                i = sql.attributes.index(attribute)
                if new_data.get(attribute) is not None:
                    new_data[attribute] = sql.update_values[i][1:-1]
                else:
                    print("Error, wrong column name!")
            database.tree[pk] = new_data
        databases[sql.table] = database


def or_condition(result, data, pk, condition):
    if condition["operator"] == "=":
        if data[condition["column"]] == condition["value"]:
            if result.get(pk) is None:
                result[pk] = data
    elif condition["operator"] == ">":
        if data[condition["column"]] > condition["value"]:
            if result.get(pk) is None:
                result[pk] = data
    elif condition["operator"] == "<":
        if data[condition["column"]] < condition["value"]:
            if result.get(pk) is None:
                result[pk] = data
    elif condition["operator"] == ">=":
        if data[condition["column"]] >= condition["value"]:
            if result.get(pk) is None:
                result[pk] = data
    elif condition["operator"] == "<=":
        if data[condition["column"]] <= condition["value"]:
            if result.get(pk) is None:
                result[pk] = data


def and_condition(result, data, pk, condition):
    if condition["operator"] == "=":
        if data[condition["column"]] != condition["value"]:
            del result[pk]
    elif condition["operator"] == ">":
        if data[condition["column"]] <= condition["value"]:
            del result[pk]
    elif condition["operator"] == "<":
        if data[condition["column"]] < condition["value"]:
            del result[pk]
    elif condition["operator"] == ">=":
        if data[condition["column"]] < condition["value"]:
            del result[pk]
    elif condition["operator"] == "<=":
        if data[condition["column"]] > condition["value"]:
            del result[pk]


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

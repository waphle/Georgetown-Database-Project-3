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
        for insert_values in sql.insert_values:
            data = {}
            for col in database.attribute:
                if col in sql.attributes:
                    if database.type[database.attribute.index(col)] == "INT":
                        data[col] = int(insert_values[sql.attributes.index(col)])
                    else:
                        data[col] = insert_values[sql.attributes.index(col)][1:-1]
                else:
                    if database.type[database.attribute.index(col)] == "INT":
                        if database.default[database.attribute.index(col)] is not None:
                            data[col] = int(database.default[database.attribute.index(col)])
                        else:
                            data[col] = None
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
        if sql.conditions:
            result = result_from_conditions(sql.conditions, database)
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

    elif sql.operation == "DELETE":
        if databases.get(sql.table) is not None:
            database = databases.get(sql.table)
        else:
            print("Table not exist!")
            return
        if sql.conditions:
            result = result_from_conditions(sql.conditions, database)
        else:
            result = database.tree
        for pk, data in result.items():
            del database.tree[pk]
        databases[sql.table] = database

    elif sql.operation == "SELECT":
        if databases.get(sql.table) is not None:
            database = databases.get(sql.table)
        else:
            print("Table not exist!")
            return
        # check column
        check_list = sql.attributes.copy()
        if sql.order_by is not None:
            for col in sql.order_by:
                if col[-5:] == " DESC":
                    check_list.append(col[:-5])
                elif col[-4:] == " ASC":
                    check_list.append(col[:-4])
                else:
                    check_list.append(col)
        for col in check_list:
            if col == "*":
                continue
            if col[:4].upper() == "SUM(" or col[:4].upper() == "AVG(":
                new_col = col[4:-1]
            elif col[:4].upper() == "MIN(" or col[:4].upper() == "MAX(":
                new_col = col[4:-1]
            elif col[:6].upper() == "COUNT(":
                new_col = col[6:-1]
            else:
                new_col = col
            if new_col not in database.attribute:
                print("Error, wrong column name!")
                return

        if sql.conditions:
            result = result_from_conditions(sql.conditions, database)
        else:
            result = database.tree
        if sql.group_by is not None:
            # todo
            print(111)
        else:
            keys = []
            orders = []

            if sql.order_by is not None:
                for col in sql.order_by:
                    if col[-5:] == " DESC":
                        keys.append(col[:-5])
                        orders.append(-1)
                    elif col[-4:] == " ASC":
                        keys.append(col[:-4])
                        orders.append(1)
                    else:
                        keys.append(col)
                        orders.append(1)
            else:
                for col in database.PK:
                    keys.append(col)
                    orders.append(1)

            output = []
            if sql.attributes == ["*"]:
                for _, data in result.items():
                    output.append(data)
                output = sorted(output, key=lambda x: tuple(orders[k] * x[c] for k, c in enumerate(keys)))
            else:
                for _, data in result.items():
                    new_data = {}
                    for col in sql.attributes:
                        if col == "*":
                            for sub_col in database.attribute:
                                new_data[sub_col] = data[sub_col]
                        elif col[:4].upper() == "SUM(":
                            new_col = col[4:-1]
                            new_data[col] = sum(d[new_col] for d in list(result.values()))
                        elif col[:4].upper() == "AVG(":
                            new_col = col[4:-1]
                            new_data[col] = float(sum(d[new_col] for d in list(result.values())) / len(result))
                        elif col[:4].upper() == "MIN(":
                            new_col = col[4:-1]
                            new_data[col] = min(result.values(), key=lambda d: d[new_col])[new_col]
                        elif col[:4].upper() == "MAX(":
                            new_col = col[4:-1]
                            new_data[col] = max(result.values(), key=lambda d: d[new_col])[new_col]
                        elif col[:6].upper() == "COUNT(":
                            new_data[col] = len(result)
                        else:
                            new_data[col] = data[col]
                    output.append(new_data)
                output = sorted(output, key=lambda x: tuple(orders[k] * x[c] for k, c in enumerate(keys)))

            for out in output:
                print(out)


def result_from_conditions(conditions, database):
    result = OOBTree()
    f = 0
    for condition in conditions:
        new_condition = condition.copy()
        if condition != "AND" and condition != "OR":
            if condition["column"] not in database.attribute:
                print("Error, wrong column name!")
            i = database.attribute.index(condition["column"])
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
    return result


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

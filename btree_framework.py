from BTrees.OOBTree import OOBTree

# References:
# https://www.tutorialspoint.com/python/python_nodes.htm
# https://inst.eecs.berkeley.edu/~cs61a/fa15/lab/lab05/
# https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.tree.recognition.is_tree.html

# list saved in main memory.
databases = {}


class Table:
    def __init__(self, sql) -> None:
        if sql is None:
            self.tree = OOBTree()
            self.attribute = []
            self.type = []
            self.PK = []
            self.FK = []
            self.FK_table = []
        else:
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
                    if database.type[database.attribute.index(attribute)] != 'INT':
                        new_data[attribute] = sql.update_values[i][1:-1]
                    else:
                        new_data[attribute] = int(sql.update_values[i])
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
        if sql.group_by is not None:
            for col in sql.group_by:
                if col not in database.attribute:
                    print("Error, wrong column name in group by!")
                    return
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
                elif sql.order is not None:
                    keys.append(col)
                    orders.append(sql.order)
                else:
                    keys.append(col)
                    orders.append(1)
        else:
            for col in database.PK:
                keys.append(col)
                orders.append(1)

        if sql.group_by is not None:
            grouped_data = {}
            for _, data in result.items():
                key = tuple(data[col] for col in sql.group_by)
                if key not in grouped_data:
                    grouped_data[key] = []
                grouped_data[key].append(data)
            output = []
            output_type = []
            for key, group in grouped_data.items():
                new_data = dict(zip(sql.group_by, key))
                for col in sql.group_by:
                    output_type.append(database.type[database.attribute.index(col)])
                f = 0
                for col in sql.attributes:
                    if col in key:
                        continue
                    elif col[:4].upper() == "SUM(":
                        if f == -1:
                            print("Error, can't group by")
                            return
                        f = 1
                        new_col = col[4:-1]
                        new_data[col] = sum(d[new_col] for d in list(group))
                    elif col[:4].upper() == "AVG(":
                        if f == -1:
                            print("Error, can't group by")
                            return
                        f = 1
                        new_col = col[4:-1]
                        new_data[col] = float(sum(d[new_col] for d in list(group)) / len(group))
                    elif col[:4].upper() == "MIN(":
                        if f == -1:
                            print("Error, can't group by")
                            return
                        f = 1
                        new_col = col[4:-1]
                        new_data[col] = min(group, key=lambda d: d[new_col])[new_col]
                    elif col[:4].upper() == "MAX(":
                        if f == -1:
                            print("Error, can't group by")
                            return
                        f = 1
                        new_col = col[4:-1]
                        new_data[col] = max(group, key=lambda d: d[new_col])[new_col]
                    elif col[:6].upper() == "COUNT(":
                        if f == -1:
                            print("Error, can't group by")
                            return
                        f = 1
                        new_col = col[6:-1]
                        new_data[col] = len(group)
                    elif col == "*":
                        if f == 1:
                            print("Error, can't group by")
                            return
                        f = -1
                        for sub_col in database.attribute:
                            new_data[sub_col] = group[0][sub_col]
                            output_type.append(database.type[database.attribute.index(sub_col)])
                        continue
                    else:
                        if f == 1:
                            print("Error, can't group by")
                            return
                        f = -1
                        new_col = col
                        new_data[col] = group[col]
                    output_type.append(database.type[database.attribute.index(new_col)])
                output.append(new_data)
        else:
            temp = []

            for _, data in result.items():
                temp.append(data)
            temp = sorted(temp, key=lambda x: tuple(orders[k] * x[c] for k, c in enumerate(keys)))

            output = []
            output_type = []
            if sql.attributes == ["*"]:
                output = temp
            else:
                for data in temp:
                    new_data = {}
                    for col in sql.attributes:
                        if col == "*":
                            for sub_col in database.attribute:
                                new_data[sub_col] = data[sub_col]
                                output_type.append(database.type[database.attribute.index(sub_col)])
                            continue
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
                            new_col = col[6:-1]
                            new_data[col] = len(result)
                        else:
                            new_col = col
                            new_data[col] = data[col]
                        output_type.append(database.type[database.attribute.index(new_col)])
                    output.append(new_data)

        new_output = []
        if sql.having is not None and output != []:
            f = 0
            for condition in sql.having:
                new_condition = {}
                if condition != "AND" and condition != "OR":
                    new_condition = condition.copy()
                    if condition["column"] not in output[0].keys():
                        print("Error, wrong column name!")
                    i = 0
                    for key in output[0].keys():
                        if key == condition["column"]:
                            break
                        i += 1
                    if new_condition["value"] in database.attribute or new_condition["value"] in output[0].keys():
                        new_condition["column2"] = new_condition["value"]
                        new_condition["value"] = None
                    elif output_type[i] == "INT":
                        new_condition["value"] = int(condition["value"])
                    else:
                        new_condition["value"] = condition["value"][1:-1]
                if condition == "AND":
                    f = 1
                elif condition == "OR":
                    f = 0
                elif f == 0:
                    for data in output:
                        or_having(new_output, data, new_condition)
                elif f == 1:
                    temp = new_output.copy()
                    for data in new_output:
                        and_having(temp, data, new_condition)
                    new_output = temp.copy()

            if new_output:
                new_keys = keys.copy()
                for key in keys:
                    i = new_keys.index(key)
                    if key not in new_output[0].keys():
                        del new_keys[i]
                        del orders[i]
                if new_keys:
                    new_output = sorted(new_output, key=lambda x: tuple(orders[k] * x[c] for k, c in enumerate(keys)))
        else:
            new_output = output.copy()

        check = []
        for out in new_output:
            if out not in check:
                check.append(out)
        i = 0
        for out in check:
            i += 1
            print(out)
            if sql.limit == i:
                break
        print("Totally " + str(len(check)) + " line(s).")
        print("Print " + str(i) + " line(s)")

    elif sql.operation == "JOIN":
        database1 = databases.get(sql.table)
        table1 = sql.table
        database2 = databases.get(sql.joint)
        table2 = sql.joint

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
                elif sql.order is not None:
                    keys.append(col)
                    orders.append(sql.order)
                else:
                    keys.append(col)
                    orders.append(1)
        else:
            for col in database1.PK:
                keys.append(table1 + "." + col)
                orders.append(1)

        if database1 is None or database2 is None:
            print("No such database!")
            return

        attributes = []
        for attribute in database1.attribute:
            attributes.append(table1 + "." + attribute)
        for attribute in database2.attribute:
            attributes.append(table2 + "." + attribute)
        pk1 = []
        pk2 = []
        for pk in database1.PK:
            pk1.append(table1 + "." + pk)
        for pk in database2.PK:
            pk2.append(table2 + "." + pk)
        new_attribute = []
        for attribute in sql.attributes:
            if attribute == '*':
                for col in database1.attribute:
                    new_attribute.append(table1 + '.' + col)
                for col in database2.attribute:
                    new_attribute.append(table2 + '.' + col)
                continue
            if "." in attribute:
                parts = attribute.split(".")
                if parts[0] == table1 and parts[1] in database1.attribute:
                    continue
                elif parts[0] == table2 and parts[1] in database2.attribute:
                    continue
                else:
                    print("Wrong column name!")
                    return
            else:
                print("Wrong column name! Which table you mean?")
                return

        # optimizer on sort choice
        f = 0
        for conditions in sql.on:
            if conditions["operator"] != "=":
                f = 0
                break
            if (conditions["column"] in pk1 and conditions["value"] in pk2) or \
                    (conditions["column"] in pk2 and conditions["value"] in pk1):
                f = 1
            else:
                f = 0
                break
        if len(database1.tree) < 10000 and len(database2.tree) < 10000:
            f = 0

        # merge sort
        if f == 1:
            keys1 = []
            orders1 = []
            keys2 = []
            orders2 = []
            for col in database1.PK:
                keys1.append(table1 + "." + col)
                orders1.append(1)
            for col in database2.PK:
                keys2.append(table2 + "." + col)
                orders2.append(1)

            result_type = []
            output1 = []
            for _, data in database1.tree.items():
                new_data = {}
                for key, value in data.items():
                    new_data[table1 + '.' + key] = value
                    result_type.append(database1.type[database1.attribute.index(key)])
                output1.append(new_data)
            output2 = []
            for _, data in database2.tree.items():
                new_data = {}
                for key, value in data.items():
                    new_data[table2 + '.' + key] = value
                    result_type.append(database2.type[database2.attribute.index(key)])
                output2.append(new_data)
            output1 = sorted(output1, key=lambda x: tuple(orders1[k] * x[c] for k, c in enumerate(keys1)))
            output2 = sorted(output2, key=lambda x: tuple(orders2[k] * x[c] for k, c in enumerate(keys2)))
            p1 = 0
            p2 = 0

            result = []

            while p1 < len(output1) and p2 < len(output2):
                for condition in sql.on:
                    f = 0
                    if condition["column"] in pk1:
                        if output1[p1][condition["column"]] == output2[p2][condition["value"]]:
                            f = 0
                        elif output1[p1][condition["column"]] < output2[p2][condition["value"]]:
                            f = 1
                            break
                        else:
                            f = 2
                            break
                    else:
                        if output2[p2][condition["column"]] == output1[p1][condition["value"]]:
                            f = 0
                        elif output2[p2][condition["column"]] > output1[p1][condition["value"]]:
                            f = 1
                            break
                        else:
                            f = 2
                            break
                if f == 0:
                    new_data = {}
                    new_data.update(output1[p1])
                    new_data.update(output2[p2])
                    result.append(new_data)
                    p1 += 1
                    p2 += 1
                elif f == 1:
                    p1 += 1
                elif f == 2:
                    p2 += 1

        # nested-loop
        else:
            result = []
            result_type = []
            for _, data1 in database1.tree.items():
                new_data1 = {}
                for key, value in data1.items():
                    new_data1[table1 + '.' + key] = value
                    result_type.append(database1.type[database1.attribute.index(key)])
                for _, data2 in database2.tree.items():
                    new_data2 = {}
                    for key, value in data2.items():
                        new_data2[table2 + '.' + key] = value
                        result_type.append(database2.type[database2.attribute.index(key)])
                    new_data = {}
                    new_data.update(new_data1)
                    new_data.update(new_data2)
                    result.append(new_data)
            result = result_from_on(sql.on, result, result_type, attributes)

        output = []
        for data in result:
            new_data = {}
            for attribute in sql.attributes:
                if attribute == '*':
                    new_data = data.copy()
                    break
                new_data[attribute] = data[attribute]
            output.append(new_data)
        new_output = []
        if sql.conditions is not None and output != []:
            f = 0
            for condition in sql.conditions:
                new_condition = {}
                if condition != "AND" and condition != "OR":
                    new_condition = condition.copy()
                    if condition["column"] not in output[0].keys():
                        print("Error, wrong column name!")
                    i = 0
                    for key in output[0].keys():
                        if key == condition["column"]:
                            break
                        i += 1
                    if new_condition["value"] in output[0].keys():
                        new_condition["column2"] = new_condition["value"]
                        new_condition["value"] = None
                    elif result_type[i] == "INT":
                        new_condition["value"] = int(condition["value"])
                    else:
                        new_condition["value"] = condition["value"][1:-1]
                if condition == "AND":
                    f = 1
                elif condition == "OR":
                    f = 0
                elif f == 0:
                    for data in output:
                        or_having(new_output, data, new_condition)
                elif f == 1:
                    temp = new_output.copy()
                    for data in new_output:
                        and_having(temp, data, new_condition)
                    new_output = temp.copy()
        else:
            new_output = output.copy()

        if new_output:
            new_keys = keys.copy()
            for key in keys:
                i = new_keys.index(key)
                if key not in new_output[0].keys():
                    del new_keys[i]
                    del orders[i]
            if new_keys:
                new_output = sorted(new_output, key=lambda x: tuple(orders[k] * x[c] for k, c in enumerate(keys)))

        for data in new_output:
            print(data)


def result_from_on(conditions, result, result_type, attributes):
    output = []
    if conditions is not None and result != []:
        f = 0
        for condition in conditions:
            new_condition = {}
            if condition != "AND" and condition != "OR":
                new_condition = condition.copy()
                if condition["column"] not in result[0].keys():
                    print("Error, wrong column name!")
                i = 0
                for key in result[0].keys():
                    if key == condition["column"]:
                        break
                    i += 1
                if new_condition["value"] in attributes:
                    new_condition["column2"] = new_condition["value"]
                    new_condition["value"] = None
                elif result_type[i] == "INT":
                    new_condition["value"] = int(condition["value"])
                else:
                    new_condition["value"] = condition["value"][1:-1]
            if condition == "AND":
                f = 1
            elif condition == "OR":
                f = 0
            elif f == 0:
                for data in result:
                    or_having(output, data, new_condition)
            elif f == 1:
                temp = output.copy()
                for data in output:
                    or_having(temp, data, new_condition)
                output = temp.copy()
    else:
        output = result.copy()
    return output


def result_from_conditions(conditions, database):
    result = OOBTree()
    f = 0
    for condition in conditions:
        new_condition = {}
        if condition != "AND" and condition != "OR":
            new_condition = condition.copy()
            if condition["column"] not in database.attribute:
                print("Error, wrong column name!")
            i = database.attribute.index(condition["column"])
            if new_condition["value"] in database.attribute:
                new_condition["column2"] = new_condition["value"]
                new_condition["value"] = None
            elif database.type[i] == "INT":
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
        if condition["value"] is not None:
            if data[condition["column"]] == condition["value"]:
                if result.get(pk) is None:
                    result[pk] = data
        else:
            if data[condition["column"]] == data[condition["column2"]]:
                if result.get(pk) is None:
                    result[pk] = data
    elif condition["operator"] == ">":
        if condition["value"] is not None:
            if data[condition["column"]] > condition["value"]:
                if result.get(pk) is None:
                    result[pk] = data
        else:
            if data[condition["column"]] > data[condition["column2"]]:
                if result.get(pk) is None:
                    result[pk] = data
    elif condition["operator"] == "<":
        if condition["value"] is not None:
            if data[condition["column"]] < condition["value"]:
                if result.get(pk) is None:
                    result[pk] = data
        else:
            if data[condition["column"]] < data[condition["column2"]]:
                if result.get(pk) is None:
                    result[pk] = data
    elif condition["operator"] == ">=":
        if condition["value"] is not None:
            if data[condition["column"]] >= condition["value"]:
                if result.get(pk) is None:
                    result[pk] = data
        else:
            if data[condition["column"]] >= data[condition["column2"]]:
                if result.get(pk) is None:
                    result[pk] = data
    elif condition["operator"] == "<=":
        if condition["value"] is not None:
            if data[condition["column"]] <= condition["value"]:
                if result.get(pk) is None:
                    result[pk] = data
        else:
            if data[condition["column"]] <= data[condition["column2"]]:
                if result.get(pk) is None:
                    result[pk] = data
    elif condition["operator"] == "<>" or condition["operator"] == "!=":
        if condition["value"] is not None:
            if data[condition["column"]] != condition["value"]:
                if result.get(pk) is None:
                    result[pk] = data
        else:
            if data[condition["column"]] != data[condition["column2"]]:
                if result.get(pk) is None:
                    result[pk] = data


def and_condition(result, data, pk, condition):
    if condition["operator"] == "=":
        if condition["value"] is not None:
            if data[condition["column"]] != condition["value"]:
                del result[pk]
        else:
            if data[condition["column"]] != data[condition["column2"]]:
                del result[pk]
    elif condition["operator"] == ">":
        if condition["value"] is not None:
            if data[condition["column"]] <= condition["value"]:
                del result[pk]
        else:
            if data[condition["column"]] <= data[condition["column2"]]:
                del result[pk]
    elif condition["operator"] == "<":
        if condition["value"] is not None:
            if data[condition["column"]] >= condition["value"]:
                del result[pk]
        else:
            if data[condition["column"]] >= data[condition["column2"]]:
                del result[pk]
    elif condition["operator"] == ">=":
        if condition["value"] is not None:
            if data[condition["column"]] < condition["value"]:
                del result[pk]
        else:
            if data[condition["column"]] < data[condition["column2"]]:
                del result[pk]
    elif condition["operator"] == "<=":
        if condition["value"] is not None:
            if data[condition["column"]] > condition["value"]:
                del result[pk]
        else:
            if data[condition["column"]] > data[condition["column2"]]:
                del result[pk]
    elif condition["operator"] == "!=" or condition["operator"] == "<>":
        if condition["value"] is not None:
            if data[condition["column"]] == condition["value"]:
                del result[pk]
        else:
            if data[condition["column"]] == data[condition["column2"]]:
                del result[pk]


def or_having(output, data, condition):
    if condition["operator"] == "=":
        if condition["value"] is not None:
            if data[condition["column"]] == condition["value"]:
                output.append(data)
        else:
            if data[condition["column"]] == data[condition["column2"]]:
                output.append(data)
    elif condition["operator"] == ">":
        if condition["value"] is not None:
            if data[condition["column"]] > condition["value"]:
                output.append(data)
        else:
            if data[condition["column"]] > data[condition["column2"]]:
                output.append(data)
    elif condition["operator"] == "<":
        if condition["value"] is not None:
            if data[condition["column"]] < condition["value"]:
                output.append(data)
        else:
            if data[condition["column"]] < data[condition["column2"]]:
                output.append(data)
    elif condition["operator"] == ">=":
        if condition["value"] is not None:
            if data[condition["column"]] >= condition["value"]:
                output.append(data)
        else:
            if data[condition["column"]] >= data[condition["column2"]]:
                output.append(data)
    elif condition["operator"] == "<=":
        if condition["value"] is not None:
            if data[condition["column"]] <= condition["value"]:
                output.append(data)
        else:
            if data[condition["column"]] <= data[condition["column2"]]:
                output.append(data)
    elif condition["operator"] == "!=" or condition["operator"] == "<>":
        if condition["value"] is not None:
            if data[condition["column"]] != condition["value"]:
                output.append(data)
        else:
            if data[condition["column"]] != data[condition["column2"]]:
                output.append(data)


def and_having(output, data, condition):
    if condition["operator"] == "=":
        if condition["value"] is not None:
            if data[condition["column"]] != condition["value"]:
                output.remove(data)
        else:
            if data[condition["column"]] != data[condition["column2"]]:
                output.remove(data)
    elif condition["operator"] == ">":
        if condition["value"] is not None:
            if data[condition["column"]] <= condition["value"]:
                output.remove(data)
        else:
            if data[condition["column"]] <= data[condition["column2"]]:
                output.remove(data)
    elif condition["operator"] == "<":
        if condition["value"] is not None:
            if data[condition["column"]] >= condition["value"]:
                output.remove(data)
        else:
            if data[condition["column"]] >= data[condition["column2"]]:
                output.remove(data)
    elif condition["operator"] == ">=":
        if condition["value"] is not None:
            if data[condition["column"]] < condition["value"]:
                output.remove(data)
        else:
            if data[condition["column"]] < data[condition["column2"]]:
                output.remove(data)
    elif condition["operator"] == "<=":
        if condition["value"] is not None:
            if data[condition["column"]] > condition["value"]:
                output.remove(data)
        else:
            if data[condition["column"]] > data[condition["column2"]]:
                output.remove(data)
    elif condition["operator"] == "!=" or condition["operator"] == "<>":
        if condition["value"] is not None:
            if data[condition["column"]] == condition["value"]:
                output.remove(data)
        else:
            if data[condition["column"]] == data[condition["column2"]]:
                output.remove(data)

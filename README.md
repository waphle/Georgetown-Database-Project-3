# Georgetown Database Project 3
 
### Group members: Zhaoming Wang, Yunqi Du, and Jeffrey Zhang.

# SQL Handler

The SQL Handler is a Python module that can handle SQL queries base on the SQL parser. It can handle SELECT, INSERT, UPDATE, DROP and DELETE queries, as well as single relation operations with GROUP BY and HAVING clauses and JOIN.

## Usage

To use the SQL Handler, simply import the btree_framework module and pass a parsed SQL class to the operation():

```python
import btree_framework
from sqlParser import SQL
btree_framework.operation(sql)

sql = SQL("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(255), age INT)")
btree_framework.operation(sql)
sql = SQL("INSERT INTO employees (id, name, age) VALUES (1, 'John Smith', 30)")
btree_framework.operation(sql)
sql = SQL("INSERT INTO employees (id, name, age) VALUES (2, 'Jane Doe', 28);")
btree_framework.operation(sql)
```

This is a example for creating a table name employees.

## Methods
The SQL Parser provides the following methods:

Table(): This class is used to create a new table object with attributes such as the tree, attribute types, primary and foreign keys.
operation(): This function is used to perform various SQL operations on the table object.

## Unit Tests
The SQL Parser module includes a set of unit tests that cover all primary single relation operations, including order by and having, and "AND" and "OR" conditions, and single aggregation operator. These unit tests are located in the test_sql_parser.py file and can be run using the following command:

```bash
python -m unittest btree_test.py
```

You can run these tests to ensure that the SQL handler module is working correctly and to verify that any changes you make to the code do not introduce new bugs. 

# SQL Parser

This is a Python module that can parse SQL queries and extract information such as the operation type, table name, selected attributes, conditions, and order by clause. It can handle SELECT, INSERT, UPDATE, and DELETE queries, as well as single relation operations with GROUP BY and HAVING clauses.

## Usage

To use the SQL Parser, simply import the `SQL` class from the `sqlParser` module and pass a raw SQL query string to the constructor:

```python
from sqlParser import SQL

query = "SELECT column1, column2 FROM my_table WHERE column1 = 'value1' ORDER BY column2;"
sql = SQL(query)

print(sql.operation)  # SELECT
print(sql.attributes)  # ['column1', 'column2']
print(sql.table)  # my_table
print(sql.conditions)  # [{'column': 'column1', 'operator': '=', 'value': "'value1'"}]
print(sql.order_by)  # column2
```

## Methods
parse_conditions(self, where_token)
This method parses the conditions in a SQL query.

__init__(self, raw_sql)
This method initializes a new instance of the SQL class and parses the SQL query.

## Unit Tests
The SQL Parser module includes a set of unit tests that cover all primary single relation operations, including order by and having, and "AND" and "OR" conditions, and single aggregation operator. These unit tests are located in the test_sql_parser.py file and can be run using the following command:

```bash
python -m unittest parser_test.py
```

You can run these tests to ensure that the SQL Parser module is working correctly and to verify that any changes you make to the code do not introduce new bugs. 

# Data save
This is a link to let our code to save the databases into the main memory or remote port.

## Usage
use the Redis to do the saving

# Optimizer

This project contains two optimizers. Conjunctive and disjunctive condition ordering and Merge-Sort vs. Nested-Loop join selection

##Rule-Based Optimization (RBO)

We make a simple RBO to reorder the Conjunctive  and disjunctive condition 

##Merge-Sort
We check the size of two table and check whether the condition is PK.If is PK and size is both large we use Merge-Sort, other wise we use Nested-Loop join.

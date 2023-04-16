# Georgetown Database Project 3
 
### Group members: Zhaoming Wang, Yunqi Du, and Jeffrey Zhang.


# SQL Parser

This is a Python module that can parse SQL queries and extract information such as the operation type, table name, selected attributes, conditions, and order by clause. It can handle SELECT, INSERT, UPDATE, and DELETE queries, as well as single relation operations with GROUP BY and HAVING clauses.

## Usage

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
python -m unittest test_sql_parser.py
```

Here's an overview of the test cases that are included:

test_select_query: This test case covers a basic SELECT query with conditions and an order by clause.

test_insert_query: This test case covers a basic INSERT query.

test_update_query: This test case covers a basic UPDATE query with a condition.

test_delete_query: This test case covers a basic DELETE query with a condition.

test_select_query_with_having: This test case covers a SELECT query with a GROUP BY and HAVING clause.

test_select_query_with_or_condition: This test case covers a SELECT query with an OR condition.

test_select_query_with_and_condition: This test case covers a SELECT query with an AND condition.

test_select_query_with_single_aggregation_operator: This test case covers a SELECT query with a single aggregation operator.

You can run these tests to ensure that the SQL Parser module is working correctly and to verify that any changes you make to the code do not introduce new bugs. 
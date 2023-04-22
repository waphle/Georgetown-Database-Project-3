import btree_framework
from sqlParser import SQL

'''
sql = "CREATE TABLE users (id INT NOT NULL, name VARCHAR(50) NOT NULL, email VARCHAR(100) NOT NULL, PRIMARY KEY (id));"
btree_framework.operation(SQL(sql))
sql = "INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john.doe@example.com');"
btree_framework.operation(SQL(sql))
sql = "INSERT INTO users (id, name, email) VALUES (2, 'Jane Smith', 'jane.smith@example.com');"
btree_framework.operation(SQL(sql))
print(btree_framework.databases["users"].tree.get(1))
print(btree_framework.databases["users"].tree.get(2))
sql = "UPDATE users SET email = 'new_email@example.com' WHERE id = 1;"
btree_framework.operation(SQL(sql))
print(btree_framework.databases["users"].tree.get(1))
print(btree_framework.databases["users"].tree.get(2))
sql = "UPDATE users SET email = 'no_email@example.com';"
btree_framework.operation(SQL(sql))
print(btree_framework.databases["users"].tree.get(1))
print(btree_framework.databases["users"].tree.get(2))
sql = "UPDATE users SET name = 'James' WHERE email = 'no_email@example.com' AND id = 1 OR name = 'John Doe';"
btree_framework.operation(SQL(sql))
print(btree_framework.databases["users"].tree.get(1))
print(btree_framework.databases["users"].tree.get(2))
sql = "DELETE FROM users WHERE name = 'James';"
btree_framework.operation(SQL(sql))
print(btree_framework.databases["users"].tree.get(1))
print(btree_framework.databases["users"].tree.get(2))
'''
sql = "CREATE TABLE example_table (id INT, grade INT);"
btree_framework.operation(SQL(sql))
sql = "ALTER TABLE example_table ADD PRIMARY KEY (id);"
btree_framework.operation(SQL(sql))
sql = "INSERT INTO example_table (id, grade) VALUES (1, 10), (2, 20), (3, 15), (4, 5), (5, 30);"
btree_framework.operation(SQL(sql))
print(btree_framework.databases["example_table"].tree.get(1))
print(btree_framework.databases["example_table"].tree.get(2))
print(btree_framework.databases["example_table"].tree.get(3))
print(btree_framework.databases["example_table"].tree.get(4))
print(btree_framework.databases["example_table"].tree.get(5))
sql = "SELECT * FROM example_table;"
btree_framework.operation(SQL(sql))

print("------------------------")

sql = "SELECT * FROM example_table WHERE id >= 3;"
btree_framework.operation(SQL(sql))

print("------------------------")

sql = "SELECT *, AVG(grade) FROM example_table ORDER BY grade;"
btree_framework.operation(SQL(sql))
print("------------------------")

sql = "INSERT INTO example_table (id, grade) VALUES (6, 10), (7, 20), (8, 15), (9, 5), (10, 30);"
btree_framework.operation(SQL(sql))

print("------------------------")
sql = "SELECT SUM(id) FROM example_table GROUP BY grade;"
btree_framework.operation(SQL(sql))

print("------------------------")
sql = "CREATE TABLE class_grade (id INT, grade INT, class_no INT);"
btree_framework.operation(SQL(sql))
sql = "ALTER TABLE class_grade ADD PRIMARY KEY (id);"
btree_framework.operation(SQL(sql))
sql = "INSERT INTO class_grade (id, grade, class_no) VALUES (1, 10, 1), (2, 20, 1), (3, 15, 1), (4, 5, 2), (5, 30, 2);"
btree_framework.operation(SQL(sql))
print("------------------------")

sql = "SELECT * FROM class_grade;"
btree_framework.operation(SQL(sql))

print("------------------------")
sql = "SELECT AVG(grade) FROM class_grade GROUP BY class_no;"
btree_framework.operation(SQL(sql))
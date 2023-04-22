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
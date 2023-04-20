import btree_framework
from sqlParser import SQL

sql = "CREATE TABLE users (id INT NOT NULL, name VARCHAR(50) NOT NULL, email VARCHAR(100) NOT NULL, PRIMARY KEY (id));"
btree_framework.operation(SQL(sql))
sql = "INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john.doe@example.com');"
btree_framework.operation(SQL(sql))
sql = "INSERT INTO users (id, name, email) VALUES (2, 'Jane Smith', 'jane.smith@example.com');"
btree_framework.operation(SQL(sql))
print(btree_framework.databases["users"].tree.get(1))
print(btree_framework.databases["users"].tree.get(2))
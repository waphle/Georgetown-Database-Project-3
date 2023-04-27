import unittest
from sqlParser import SQL
import btree_framework


class TestSQL(unittest.TestCase):
    def test_create_table(self):
        sql = SQL("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(255), age INT)")
        btree_framework.operation(sql)
        self.assertEqual("employees" in btree_framework.databases.keys(), True)
        self.assertEqual(btree_framework.databases.get("employees").PK, ["id"])
        self.assertEqual(btree_framework.databases.get("employees").type, ["INT", "VARCHAR(255)", "INT"])

    def test_insert_into_table(self):
        # sql = SQL("DROP TABLE employees")
        # btree_framework.operation(sql)
        sql = SQL("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(255), age INT)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (1, 'John Smith', 30)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (2, 'Jane Doe', 28);")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (3, 'Bob Johnson', 45)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (4, 'Alice Davis', 22)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (5, 'Tom Wilson', 50)")
        btree_framework.operation(sql)
        self.assertEqual(btree_framework.databases.get("employees").tree.get(1),
                         {"id": 1, "name": 'John Smith', "age": 30})
        self.assertEqual(btree_framework.databases.get("employees").tree.get(2),
                         {"id": 2, "name": 'Jane Doe', "age": 28})
        self.assertEqual(btree_framework.databases.get("employees").tree.get(3),
                         {"id": 3, "name": 'Bob Johnson', "age": 45})
        self.assertEqual(btree_framework.databases.get("employees").tree.get(4),
                         {"id": 4, "name": 'Alice Davis', "age": 22})
        self.assertEqual(btree_framework.databases.get("employees").tree.get(5),
                         {"id": 5, "name": 'Tom Wilson', "age": 50})

    def test_drop_table(self):
        # sql = SQL("DROP TABLE employees")
        # btree_framework.operation(sql)
        sql = SQL("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(255), age INT)")
        btree_framework.operation(sql)
        sql = SQL("DROP TABLE employees")
        btree_framework.operation(sql)
        self.assertEqual("employees" in btree_framework.databases.keys(), False)

    def test_delete_from_table(self):
        # sql = SQL("DROP TABLE employees")
        # btree_framework.operation(sql)
        sql = SQL("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(255), age INT)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (1, 'John Smith', 30)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (2, 'Jane Doe', 28);")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (3, 'Bob Johnson', 45)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (4, 'Alice Davis', 22)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (5, 'Tom Wilson', 50)")
        btree_framework.operation(sql)
        sql = SQL("DELETE FROM employees WHERE age > 30")
        btree_framework.operation(sql)
        self.assertEqual(btree_framework.databases.get("employees").tree.get(1),
                         {"id": 1, "name": 'John Smith', "age": 30})
        self.assertEqual(btree_framework.databases.get("employees").tree.get(2),
                         {"id": 2, "name": 'Jane Doe', "age": 28})
        self.assertEqual(btree_framework.databases.get("employees").tree.get(4),
                         {"id": 4, "name": 'Alice Davis', "age": 22})
        self.assertEqual(btree_framework.databases.get("employees").tree.get(3), None)
        self.assertEqual(btree_framework.databases.get("employees").tree.get(5), None)

    def test_select_from_table(self):
        # sql = SQL("DROP TABLE employees")
        # btree_framework.operation(sql)
        sql = SQL("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(255), age INT)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (1, 'John Smith', 30)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (2, 'Jane Doe', 28);")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (3, 'Bob Johnson', 45)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (4, 'Alice Davis', 22)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (5, 'Tom Wilson', 50)")
        btree_framework.operation(sql)

        print("SELECT age > 30")

        sql = SQL("SELECT name, age FROM employees WHERE age > 30")
        btree_framework.operation(sql)

        print("Should be 3 and 5")

    def test_select_and_from_table(self):
        # sql = SQL("DROP TABLE employees")
        # btree_framework.operation(sql)
        sql = SQL("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(255), age INT)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (1, 'John Smith', 30)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (2, 'Jane Doe', 28);")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (3, 'Bob Johnson', 45)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (4, 'Alice Davis', 22)")
        btree_framework.operation(sql)
        sql = SQL("INSERT INTO employees (id, name, age) VALUES (5, 'Tom Wilson', 50)")
        btree_framework.operation(sql)

        print("SELECT age > 30")

        sql = SQL("SELECT name, age FROM employees WHERE age > 30")
        btree_framework.operation(sql)

        print("Should be 3")

    def test_group_by(self):
        sql = "CREATE TABLE class_grade (id INT, grade INT, class_no INT);"
        btree_framework.operation(SQL(sql))
        sql = "ALTER TABLE class_grade ADD PRIMARY KEY (id);"
        btree_framework.operation(SQL(sql))
        sql = "INSERT INTO class_grade (id, grade, class_no) VALUES (1, 10, 1), (2, 20, 1), (3, 15, 1), (4, 5, 2), (5, 30, 2);"
        btree_framework.operation(SQL(sql))
        sql = "SELECT *,AVG(grade) FROM class_grade HAVING AVG(grade)>grade;"
        print("students grade low than avg")
        btree_framework.operation(SQL(sql))
        print("should be 1,3,4")
        sql = "SELECT AVG(grade) FROM class_grade GROUP BY class_no ORDER BY AVG(grade) DESC;"
        print("class avg")
        btree_framework.operation(SQL(sql))
        print("should be 2 line")

    def test_join(self):
        sql = "CREATE TABLE example_table (id INT, grade INT);"
        btree_framework.operation(SQL(sql))
        sql = "ALTER TABLE example_table ADD PRIMARY KEY (id);"
        btree_framework.operation(SQL(sql))
        sql = "INSERT INTO example_table (id, grade) VALUES (1, 10), (2, 20), (3, 15), (4, 5), (5, 30);"
        btree_framework.operation(SQL(sql))

        sql = "CREATE TABLE class_no (id INT, class_no INT);"
        btree_framework.operation(SQL(sql))
        sql = "ALTER TABLE class_no ADD PRIMARY KEY (id);"
        btree_framework.operation(SQL(sql))
        sql = "INSERT INTO class_no (id, class_no) VALUES (1, 1), (2, 1), (3, 1), (4, 2), (5, 2);"
        btree_framework.operation(SQL(sql))

        print('Test Joint from class 1')
        sql = "SELECT example_table.id,example_table.grade,class_no.class_no FROM example_table JOIN class_no ON " \
              "example_table.id = class_no.id WHERE class_no.class_no = 1 ORDER BY example_table.grade DESC;"
        btree_framework.operation(SQL(sql))
        print("should be 3 lines")


if __name__ == '__main__':
    unittest.main()

import unittest
from sqlParser import SQL


class TestSQLParser(unittest.TestCase):

    def test_select_query(self):
        sql = SQL("SELECT column1, SUM(column2) FROM my_table WHERE column1 > 100 AND column3 = 'value' ORDER BY column1 DESC")
        self.assertEqual(sql.operation, "SELECT")
        self.assertEqual(sql.conditions, [
            {"column": "column1", "operator": ">", "value": "100"},
            "AND",
            {"column": "column3", "operator": "=", "value": "'value'"}
        ])
        self.assertEqual(sql.order_by, "column1 DESC")

    def test_insert_query(self):
        self.assertEqual(sql.attributes, ["column1", "SUM(column2)"])
        self.assertEqual(sql.table, "my_table")
        sql = SQL("INSERT INTO my_table (column1, column2) VALUES ('value1', 'value2')")
        self.assertEqual(sql.operation, "INSERT")
        self.assertEqual(sql.table, "my_table")
        self.assertEqual(sql.attributes, ['column1', 'column2'])
        self.assertEqual(sql.insert_values, ["'value1'", "'value2'"])

    def test_update_query(self):
        sql = SQL("UPDATE my_table SET column1 = 'value1' WHERE column2 > 100")
        self.assertEqual(sql.operation, "UPDATE")
        self.assertEqual(sql.table, "my_table")
        self.assertEqual(sql.conditions, [{"column": "column2", "operator": ">", "value": "100"}])

    def test_delete_query(self):
        sql = SQL("DELETE FROM my_table WHERE column1 = 'value1'")
        self.assertEqual(sql.operation, "DELETE")
        self.assertEqual(sql.table, "my_table")
        self.assertEqual(sql.conditions, [{"column": "column1", "operator": "=", "value": "'value1'"}])

    def test_select_query_with_having(self):
        sql = SQL("SELECT column1, SUM(column2) FROM my_table GROUP BY column1 HAVING SUM(column2) > 100")
        self.assertEqual(sql.operation, "SELECT")
        self.assertEqual(sql.attributes, ["column1", "SUM(column2)"])
        self.assertEqual(sql.table, "my_table")
        self.assertEqual(sql.having, [{'column': 'SUM(column2)', 'operator': '>', 'value': '100'}])

    def test_select_query_with_or_condition(self):
        sql = SQL("SELECT * FROM my_table WHERE column1 = 'value1' OR column2 < 100")
        self.assertEqual(sql.operation, "SELECT")
        self.assertEqual(sql.attributes, ["*"])
        self.assertEqual(sql.table, "my_table")
        self.assertEqual(sql.conditions, [
            {"column": "column1", "operator": "=", "value": "'value1'"},
            "OR",
            {"column": "column2", "operator": "<", "value": "100"}
        ])

    def test_select_query_with_and_condition(self):
        sql = SQL("SELECT * FROM my_table WHERE column1 = 'value1' AND column2 < 100")
        self.assertEqual(sql.operation, "SELECT")
        self.assertEqual(sql.attributes, ["*"])
        self.assertEqual(sql.table, "my_table")
        self.assertEqual(sql.conditions, [
            {"column": "column1", "operator": "=", "value": "'value1'"},
            "AND",
            {"column": "column2", "operator": "<", "value": "100"}
        ])

    def test_select_query_with_single_aggregation_operator(self):
        sql = SQL("SELECT COUNT(*) FROM my_table")
        self.assertEqual(sql.operation, "SELECT")
        self.assertEqual(sql.attributes, ["COUNT(*)"])
        self.assertEqual(sql.table, "my_table")

    def test_create_table_query(self):
        sql = SQL("CREATE TABLE my_table (column1 INT NOT NULL DEFAULT 1, column2 CHAR(10), column3 DATE)")
        self.assertEqual(sql.operation, "CREATE")
        self.assertEqual(sql.table, "my_table")
        self.assertEqual(sql.attributes, ["column1", "column2", "column3"])
        self.assertEqual(sql.type, ["INT", "CHAR(10)", "DATE"])
        self.assertEqual(sql.not_null, [True, False, False])
        self.assertEqual(sql.values, ["1", "", ""])

    def test_drop_table_query(self):
        sql = SQL("DROP TABLE my_table")
        self.assertEqual(sql.operation, "DROP")
        self.assertEqual(sql.table, "my_table")

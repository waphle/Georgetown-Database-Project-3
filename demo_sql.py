# Generate random SQL queries for demo
import sqlparse
import sqlParser
import random
from sqlParser import SQL

def generator(self, sqlParser):
    table = ['customers', 'orders', 'products']
    column = ['id', 'name', 'price', 'quantity']
    num_queries = 10

    for i in range(num_queries):
        rand_table = random.choice(table)
        num_column = random.randint(1, len(column))
        selected_column = random.sample(column, num_column)

        sql_query = f"SELECT {', '.join(selected_column)} FROM {rand_table};"

    print(sql_query)
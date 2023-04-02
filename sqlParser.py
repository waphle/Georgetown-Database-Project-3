# the sql parser
import sqlparse


def parse_conditions(self, where_token):
    conditions = []
    for token in where_token.tokens:
        self.operator = ""
        if isinstance(token, sqlparse.sql.Parenthesis):
            conditions.extend(parse_conditions(self, token))
        elif token.ttype == sqlparse.tokens.Keyword and token.value.upper() in ["AND", "OR"]:
            conditions.append(token.value.upper())
        elif isinstance(token, sqlparse.sql.Comparison):
            for cmp in token.tokens[1:-1]:
                if str(cmp) != " ":
                    self.operator += str(cmp)
            condition = {"column": str(token.left), "operator": self.operator,
                         "value": str(token.right)}
            conditions.append(condition)
    return conditions


class SQL:
    def __init__(self, raw_sql):
        self.error = False
        self.raw_sql = raw_sql
        self.parsed_sql = sqlparse.parse(raw_sql)[0]
        self.operation = self.parsed_sql.get_type()
        self.attributes = None
        self.table = None
        self.conditions = None
        if self.operation.upper() == 'SELECT':
            for token in self.parsed_sql.tokens:
                if token.ttype == sqlparse.tokens.Keyword.DML and token.value.upper() == "SELECT":
                    self.attributes = []
                    tmp = self.parsed_sql.tokens[self.parsed_sql.token_index(token) + 2]
                    if tmp.ttype is sqlparse.tokens.Wildcard:
                        self.attributes.append('*')
                    else:
                        for sub_token in tmp.tokens:
                            if isinstance(sub_token, sqlparse.sql.Identifier):
                                self.attributes.append(sub_token.value)
                if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "FROM":
                    self.table = \
                        self.parsed_sql.tokens[self.parsed_sql.token_index(token) + 2].get_real_name()
                elif isinstance(token, sqlparse.sql.Where):
                    self.conditions = parse_conditions(self, token)
        elif self.operation.upper() == 'INSERT':
            for token in self.parsed_sql.tokens:
                if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "INTO":
                    self.table = \
                        self.parsed_sql.tokens[self.parsed_sql.token_index(token) + 2].get_real_name()
        elif self.operation.upper() == 'UPDATE':
            for token in self.parsed_sql.tokens:
                if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "UPDATE":
                    self.table = \
                        self.parsed_sql.tokens[self.parsed_sql.token_index(token) + 2].get_real_name()
                elif isinstance(token, sqlparse.sql.Where):
                    self.conditions = parse_conditions(token)
        elif self.operation.upper() == 'DELETE':
            for token in self.parsed_sql.tokens:
                if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "FROM":
                    self.table = \
                        self.parsed_sql.tokens[self.parsed_sql.token_index(token) + 2].get_real_name()
                elif isinstance(token, sqlparse.sql.Where):
                    self.conditions = parse_conditions(token)
        else:
            self.error = True


test = "SELECT * from test where test.id=1 and test.value != 1;"
sql = SQL(test)
print(sql.operation)
print(sql.attributes)
print(sql.table)
print(sql.conditions)

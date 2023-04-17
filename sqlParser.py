# the sql parser
import sqlparse
from sqlparse.exceptions import SQLParseError


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
        try:
            self.parsed_sql = sqlparse.parse(raw_sql)[0]
        except SQLParseError as e:
            print(f"Error: {e}")
        self.error = False
        self.raw_sql = raw_sql
        self.operation = self.parsed_sql.get_type()
        self.attributes = None
        self.table = None
        self.conditions = None
        self.type = None
        self.values = None
        self.not_null = None
        self.insert_values = None
        self.update_values = None
        self.order_by = None
        self.having = None
        # print(self.parsed_sql.tokens)
        if self.operation.upper() == 'SELECT':
            for token in self.parsed_sql.tokens:
                # print(token)
                if token.ttype == sqlparse.tokens.Keyword.DML and token.value.upper() == "SELECT":
                    self.attributes = []
                    select_token = self.parsed_sql.tokens[self.parsed_sql.token_index(token) + 2]
                    if select_token.ttype is sqlparse.tokens.Wildcard:
                        self.attributes.append("*")
                    elif isinstance(select_token, (sqlparse.sql.Identifier, sqlparse.sql.Function)):
                        self.attributes.append(select_token.value)
                    elif select_token.is_group:
                        for sub_token in select_token.tokens:
                            if isinstance(sub_token, (sqlparse.sql.Identifier, sqlparse.sql.Function)):
                                self.attributes.append(sub_token.value)
                elif token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "FROM":
                    self.table = \
                        self.parsed_sql.tokens[self.parsed_sql.token_index(token) + 2].get_real_name()
                elif isinstance(token, sqlparse.sql.Where):
                    self.conditions = parse_conditions(self, token)
                elif token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "ORDER BY":
                    order_token = self.parsed_sql.tokens[self.parsed_sql.token_index(token) + 2]
                    if isinstance(order_token, (sqlparse.sql.Identifier, sqlparse.sql.Function)):
                        self.order_by = order_token.value
                    elif order_token.is_group:
                        for sub_token in order_token.tokens:
                            if isinstance(sub_token, (sqlparse.sql.Identifier, sqlparse.sql.Function)):
                                self.order_by = sub_token.value
                elif token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "HAVING":
                    self.having = []
                    p = self.parsed_sql.token_index(token) + 2
                    having_token = self.parsed_sql.tokens[p]
                    while having_token:
                        self.operator = ""
                        if isinstance(having_token, sqlparse.sql.Comparison):
                            for cmp in having_token.tokens[1:-1]:
                                if str(cmp) != " ":
                                    self.operator += str(cmp)
                            condition = {"column": str(having_token.left), "operator": self.operator,
                                         "value": str(having_token.right)}
                            self.having.append(condition)
                        elif having_token.ttype == sqlparse.tokens.Keyword and having_token.value.upper() in ["AND",
                                                                                                              "OR"]:
                            self.having.append(having_token.value.upper())
                        elif having_token.ttype == sqlparse.tokens.Whitespace:
                            pass
                        else:
                            break
                        p += 1
                        if p >= len(self.parsed_sql.tokens):
                            break
                        having_token = self.parsed_sql.tokens[p]
        elif self.operation.upper() == 'INSERT':
            for token in self.parsed_sql.tokens:
                if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "INTO":
                    self.attributes = []
                    insert_token = self.parsed_sql.tokens[self.parsed_sql.token_index(token) + 2]
                    if isinstance(insert_token, sqlparse.sql.Identifier):
                        self.table = insert_token.value
                    elif isinstance(insert_token, sqlparse.sql.Function):
                        for sub_token in insert_token.tokens:
                            if isinstance(sub_token, sqlparse.sql.Identifier):
                                self.table = sub_token.value
                            elif sub_token.is_group:
                                for sub_sub_token in sub_token.tokens:
                                    if isinstance(sub_sub_token, sqlparse.sql.Identifier):
                                        self.attributes.append(sub_sub_token.value)
                                    elif sub_sub_token.is_group:
                                        for sub_sub_sub_token in sub_sub_token.tokens:
                                            if isinstance(sub_sub_sub_token, sqlparse.sql.Identifier):
                                                self.attributes.append(sub_sub_sub_token.value)
                elif isinstance(token, sqlparse.sql.Values):
                    self.insert_values = []
                    sub_token = token.tokens[2]
                    value_token = sub_token.tokens[1]
                    for sub_token in value_token.tokens:
                        if sub_token.ttype != sqlparse.tokens.Whitespace and \
                                sub_token.ttype != sqlparse.tokens.Punctuation:
                            self.insert_values.append(sub_token.value)
        elif self.operation.upper() == 'UPDATE':
            for token in self.parsed_sql.tokens:
                if token.ttype == sqlparse.tokens.Keyword.DML and token.value.upper() == "UPDATE":
                    self.table = \
                        self.parsed_sql.tokens[self.parsed_sql.token_index(token) + 2].get_real_name()
                elif token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "SET":
                    self.update_values = []
                    update_token = self.parsed_sql.tokens[self.parsed_sql.token_index(token) + 2]
                    if isinstance(update_token, sqlparse.sql.Identifier):
                        self.update_values.append(update_token.value)
                    elif update_token.is_group:
                        for sub_token in update_token.tokens:
                            if isinstance(sub_token, sqlparse.sql.Comparison):
                                self.update_values.append(sub_token.value)
                elif isinstance(token, sqlparse.sql.Where):
                    self.conditions = parse_conditions(self, token)
        elif self.operation.upper() == 'DELETE':
            for token in self.parsed_sql.tokens:
                if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "FROM":
                    self.table = \
                        self.parsed_sql.tokens[self.parsed_sql.token_index(token) + 2].get_real_name()
                elif isinstance(token, sqlparse.sql.Where):
                    self.conditions = parse_conditions(self, token)
        elif self.operation.upper() == 'CREATE':
            self.table = self.parsed_sql.tokens[4].value
            self.attributes = []
            self.type = []
            self.values = []
            self.not_null = []
            for token in self.parsed_sql.tokens:
                if isinstance(token, sqlparse.sql.Parenthesis):
                    f = 0
                    for sub_token in token.tokens:
                        if isinstance(sub_token, sqlparse.sql.Identifier):
                            self.attributes.append(sub_token.value)
                            if f == 1:
                                self.error = True
                                return
                            if f == 2:
                                self.not_null.append(False)
                                f = 3
                            if f == 3:
                                self.values.append("")
                            f = 1
                        elif sub_token.ttype == sqlparse.tokens.Name.Builtin or \
                                isinstance(sub_token, sqlparse.sql.Function):
                            self.type.append(sub_token.value)
                            if f != 1:
                                self.error = True
                                return
                            f = 2
                        elif sub_token.ttype == sqlparse.tokens.Keyword and sub_token.value.upper() == "NOT NULL":
                            self.not_null.append(True)
                            if f != 2:
                                self.error = True
                                return
                            f = 3
                        elif sub_token.ttype == sqlparse.tokens.Keyword and sub_token.value.upper() == "DEFAULT":
                            default = token.tokens[token.token_index(sub_token) + 2]
                            if default.is_group:
                                self.values.append(default.tokens[0].value)
                            else:
                                self.values.append(default.value)
                            if f == 2:
                                self.not_null.append(False)
                            elif f != 3:
                                self.error = True
                                return
                            f = 0
                        elif isinstance(sub_token, sqlparse.sql.IdentifierList):
                            for sub_sub_token in sub_token:
                                if isinstance(sub_sub_token, sqlparse.sql.Identifier):
                                    self.attributes.append(sub_sub_token.value)
                                    if f == 1:
                                        self.error = True
                                        return
                                    if f == 2:
                                        self.not_null.append(False)
                                        f = 3
                                    if f == 3:
                                        self.values.append("")
                                    f = 1
                                elif sub_sub_token.ttype == sqlparse.tokens.Name.Builtin or \
                                        isinstance(sub_sub_token, sqlparse.sql.Function):
                                    self.type.append(sub_sub_token.value)
                                    if f != 1:
                                        self.error = True
                                        return
                                    f = 2
                                elif sub_sub_token.ttype == sqlparse.tokens.Keyword and \
                                        sub_sub_token.value.upper() == "NOT NULL":
                                    self.not_null.append(True)
                                    if f != 2:
                                        self.error = True
                                        return
                                    f = 3
                    if f == 1:
                        self.error = True
                        return
                    if f == 2:
                        self.not_null.append(False)
                        f = 3
                    if f == 3:
                        self.values.append("")
        elif self.operation.upper() == 'DROP':
            self.table = self.parsed_sql.tokens[4].value
        else:
            self.error = True



sql = SQL("CREATE TABLE my_table (column1 INT NOT NULL DEFAULT 1, column2 VARCHAR(255), column3 DATE)")
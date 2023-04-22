from sqlParser import SQL

class Condition:
    def __init__(self, column, operator, value, sqlParser):
        self.column = column
        self.operator = operator
        self.value = value
        self.sqlParser = ""

    def __repr__(self, sqlParser):
        return f"{self.column} {self.operator} {self.value} {self.sqlParser}"

# Incomplete. Needs more RBO stuff
def RBO(conditions, sqlParser):
    """
    Optimize the order of conditions based on a predefined set of rules.
    Args:
        conditions (list): List of Condition objects.
    Returns:
        list: Ordered list of Condition objects.
    """

    # Define custom sorting rules
    def sort_key(condition):
        # Rule 1: prioritize equality conditions
        if condition.operator == "=":
            return 1
        # Rule 2: prioritize range conditions (less than, greater than)
        elif condition.operator in ("<", ">", "<=", ">="):
            return 2
        # Rule 3: prioritize other conditions (like, etc.)
        else:
            return 3

    return sorted(conditions, key=sort_key)

# Example usage
conditions = [
    Condition("column1", ">", "100"),
    Condition("column2", "=", "50"),
    Condition("column3", "LIKE", "'%value%'")
]

optimized_conditions = RBO(conditions)

print("Optimized Condition Order:", optimized_conditions)
class Condition:
    def __init__(self, first, second, first_op=lambda x: x, second_op=lambda x: x):
        self.first = first
        self.second = second
        self.first_op = first_op
        self.second_op = second_op

    def get_first(self):
        return self.first

    def get_second(self):
        return self.second

    def apply(self, val1, val2):
        val1 = self.first_op(val1)
        val2 = self.second_op(val2)
        return val1 < val2

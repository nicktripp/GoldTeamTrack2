import re
import numpy


class Condition:
    def __init__(self, first, second):
        self.first = first
        self.second = second

    def get_first(self):
        return self.first

    def get_second(self):
        return self.second

    def apply(self):
        pass


class ConditionLessThan(Condition):
    def __init__(self, first, second):
        Condition.__init__(self, first, second)

    def apply(self, val1, val2):
        if numpy.isnan(val1) or numpy.isnan(val2):
            return False
        return val1 < val2


class ConditionGreaterThan(Condition):
    def __init__(self, first, second):
        Condition.__init__(self, first, second)

    def apply(self, val1, val2):
        if numpy.isnan(val1) or numpy.isnan(val2):
            return False
        return val1 > val2


class ConditionGreaterThanOrEqual(Condition):
    def __init__(self, first, second):
        Condition.__init__(self, first, second)

    def apply(self, val1, val2):
        if numpy.isnan(val1) or numpy.isnan(val2):
            return False
        return val1 >= val2


class ConditionLessThanOrEqual(Condition):
    def __init__(self, first, second):
        Condition.__init__(self, first, second)

    def apply(self, val1, val2):
        if numpy.isnan(val1) or numpy.isnan(val2):
            return False
        return val1 <= val2


class ConditionEqual(Condition):
    def __init__(self, first, second):
        Condition.__init__(self, first, second)

    def apply(self, val1, val2):
        if numpy.isnan(val1) or numpy.isnan(val2):
            return False
        return val1 == val2


class ConditionNotEqual(Condition):
    def __init__(self, first, second):
        Condition.__init__(self, first, second)

    def apply(self, val1, val2):
        if numpy.isnan(val1) or numpy.isnan(val2):
            return False
        return val1 != val2


class ConditionLike(Condition):
    def __init__(self, first, second):
        Condition.__init__(self, first, second)

    def apply(self, val1, val2):
        if numpy.isnan(val1) or numpy.isnan(val2):
            return False
        pattern = re.compile(val1)
        return pattern.match(val2)

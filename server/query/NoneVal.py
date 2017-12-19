class NoneVal:
    id = 0

    def __init__(self):
        self._id = NoneVal.id
        NoneVal.id += 1

    def __lt__(self, other):
        if isinstance(other, NoneVal):
            return self._id < other._id
        else:
            return True

    def __le__(self, other):
        return self.__lt__(other)

    def __gt__(self, other):
        if isinstance(other, NoneVal):
            return self._id > other._id
        else:
            return True

    def __ge__(self, other):
        return self.__gt__(other)

    def __eq__(self, other):
        return isinstance(other, NoneVal) and self._id == other._id

    def __hash__(self):
        return hash(self._id)

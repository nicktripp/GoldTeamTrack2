class EvaluationResult:
    """
    As condition are executed, an EvaluationResult instance will track
    the tuples that may appear in the final result
    """

    def __init__(self, tuple_length):
        self._tuple_length = tuple_length
        self._singles = {}
        self._pairs = {}
        self._pairs_lookup = {}

    def intersect_from_single(self, index, locations):
        """
        intersects the values of _singles[index] with the new set of locations
        any pairs that reference index will also be intersected with the new values

        :param index:
        :param locations:
        :return:
        """
        # Update the intersection for singles
        if index in self._singles:
            self._singles[index] &= set(locations)
        else:
            self._singles[index] = set(locations)

        # Update the intersection for any pair that includes the single
        si = self._singles[index]
        for p in self._pairs:
            vi = [(v[0] & si, v[1] & si) for v in self._pairs[p]]
            self._pairs[p] = [v for v in vi if len(v[0]) > 0 and len(v[1]) > 0]

    def add_pair(self, left_index, right_index, left_set, right_set):
        # Put the pair in order
        if left_index < right_index:
            key = (left_index, right_index)
            value = (left_set, right_set)
        else:
            key = (left_index, right_index)
            value = (right_set, left_set)

        # if there are constraints on the index, the new sets must intersect
        if key[0] in self._singles:
            value = (value[0] & self._singles[key[0]]), value[1]

        if key[1] in self._singles:
            value = (value[0], value[1] & self._singles[key[1]])

        # if there are still items in both sets, then add them to the pairs
        if len(value[0]) > 0 and len(value[1]) > 0:
            if key not in self._pairs:
                self._pairs[key] = []
            self._pairs[key] = value

    def intersect_singles_from_pairs(self):
        """
        Iterates over all pairs, collecting the set of values that
        are supported for each table
        :return:
        """
        # Update the singles entry from the list of (set, set) values in _pairs
        for l, r in self._pairs:
            # Get all of the values for an index
            acc = (set(), set())
            for value in self._pairs[(l, r)]:
                acc[0] |= value[0]
                acc[1] |= value[1]

            # All values in the pairs were already in singles or singles was empty
            self._singles[l] = acc[0]
            self._singles[r] = acc[1]

    def intersect_pairs_from_pairs(self):
        """
        Iterates through the pairs updating the value list
        for each pair such that if there exists another pair with
        one matching table index, then all the sets in the in
        value list for both pairs will have been intersected

        ie if pairs looks like this
        (0, 1) : [({100, 200, 300}, {23, 34}), ({300, 400}, {45})]
        (1, 2) : [({34, 56}, {1, 2, 3})]

        the result of the intersection looks like

        (0, 1) : [({100, 200, 300}, {34})]
        (1, 2) : [{34}, {1, 2, 3})]

        :return: None
        """
        pair_list = list(sorted(self._pairs))
        for i, p1 in enumerate(pair_list):
            l1, r1 = p1
            for j in range(i + 1, len(pair_list)):
                p2 = pair_list[j]
                l2, r2 = p2
                if l1 == l2:
                    self._intersect_pair(0, 0, p1, p2)
                if l1 == r2:
                    self._intersect_pair(0, 1, p1, p2)
                if r1 == l2:
                    self._intersect_pair(1, 0, p1, p2)
                if r1 == r2:
                    self._intersect_pair(1, 1, p1, p2)

    def _intersect_pair(self, a, b, p1, p2):
        # Compute the intersection of set a and set b (left or right 0 or 1)
        s1, s2 = set(), set()
        for v in self._pairs[p1]:
            s1 |= v[a]
        for v in self._pairs[p2]:
            s2 |= v[b]

        i12 = s1 & s2
        # Remove entries not in the intersection
        v1s = [(v[0] & i12, v[1]) for v in self._pairs[p1]]
        self._pairs[p1] = [v for v in v1s if len(v[0]) > 0]
        v2s = [(v[0], v[1] & i12) for v in self._pairs[p1]]
        self._pairs[p2] = [v for v in v2s if len(v[0]) > 0]

    def intersect(self, other):
        # Intersect the singles from other
        for index in other._singles:
            self.intersect_from_single(index, other._singles[index])

        # Intersect the pairs from other
        for p in other._pairs:
            for value in other._pairs[p]:
                self.add_pair(p[0], p[1], value[0], value[1])

            # Update the intersection following the addition of the pairs
            self.intersect_pairs_from_pairs()
            self.intersect_singles_from_pairs()

    def union(self, other):
        # TODO: actually do something
        if True:
            self._singles = other._singles
            self._pairs = other._pairs
            return

        # TODO: How to do union on this representation ... ???
        # Union the singles
        for index in other._singles:
            if index in self._singles:
                self._singles[index] |= other._singles[index]
            else:
                self._singles[index] = other._singles[index]

        # handle uniting the pairs results
        for p in other._pairs:
            if p in self._pairs:
                # TODO: handle duplicate pairs caused by extending without checking
                # Extend pairs with other's pairs
                self._pairs[p].extend(other._pairs[p])


            elif p not in self._pairs:
                self._pairs[p] = other._pairs[p]
                # TODO: self might already have constraints on these values
                # How should unions be done for this case?
                pass

    def negate(self, table_indices):
        pass

    def gen_tuples(self):
        # Create the cartesian products from the sets of viable rows
        pair_list = sorted(self._pairs)
        values = [self._pairs[p] for p in pair_list]
        window = [next(value) for value in values]

        while True:
            product_list = [None] * self._tuple_length

            # Fill the product list tuples from the pairs
            for i in range(len(pair_list)):
                p = pair_list[i]
                product_list[p[0]] = list(window[i][0])
                product_list[p[1]] = list(window[i][1])

            # Fill the empty spots with known constraints for singles (col to const conditions)
            for i in range(self._tuple_length):
                if product_list[i] is None and i in self._singles:
                    product_list[i] = list(self._singles[i])

            # Yield the cartesian product over this list of sets of locations
            yield from EvaluationResult.cartesian_generator(product_list)

            # Advance the window
            advanced = False
            i = self._tuple_length - 1
            while not advanced:
                try:
                    window[i] = next(values[i])
                    advanced = True
                except IndexError:
                        # There was no window to advance
                        return None
                except StopIteration:
                    if i == 0:
                        # We have advanced through all combinations
                        return None
                    else:
                        # Reset the iterator
                        window[i] = list(values[i])
                        i -= 1

    @staticmethod
    def cartesian_generator(tbl_rows, skip_tuples=set()):
        """
        tbl_rows is a list of lists of row start locations

        Generate the cartesian product of tbl_rows
        :param skip_tuples: tuples in this set will not be yielded
        :param tbl_rows: list of lists of row locations
        """
        idx = [0] * len(tbl_rows)
        while idx[0] < len(tbl_rows[0]):
            # build row tuple in list
            row_list = []

            # get the ith value of the next cartesian tuple
            for i in range(len(tbl_rows)):
                row_list.append(tbl_rows[i][idx[i]])

            # move the index of the cartesian tuple
            for i in reversed(range(len(tbl_rows))):
                idx[i] += 1
                # wrap the index except for the very first list in tbl_rows
                if idx[i] == len(tbl_rows[i]) and i != 0:
                    idx[i] = 0
                else:
                    break

            # don't yield tuples in the the skip_tuples set
            tup = tuple(row_list)
            if tup not in skip_tuples:
                yield tup

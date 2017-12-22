class EvaluationResult:
    """
    As condition are executed, an EvaluationResult instance will track
    the tuples that may appear in the final result

    EvaluationResult maintains a _join_map which maps pairs of table
    indices to a map of row location in the first table to a set of rows
    in the second table.

    _join_map[(i,j)] is None if there are no constraints
    indicating all tuples in the cartesian product between the tables are viable
    rows for projection.

    _join_map[(i,j)][row_from_i] is None when (row_from_i, any_row_from_j) are all
    viable tuples in the output.

    _join_map[(i,j)][row_from_i] is a set of rows from j where (row_from_i, row_from_j in set)
    are all viable tuples in the output.

    The _join_map key indices are maintained as (0,1), (1,2), (2, 3), ..., (tuple_length - 2, tuple_length - 1)
    a final (tuple_length - 1, tuple_length) maps to a dict where all keys map to None
    in order to support easier cartesian product tuple generation
    """

    def __init__(self, tuple_length):
        self._tuple_length = tuple_length

        self._join_map = {}
        for i in range(tuple_length):
            self._join_map[(i, i + 1)] = None

        # join_maps for out of order dependencies
        self._aux_deps = {}
        self._aux = {}

    def intersect_row_list(self, table_index, table_locations):
        """
        Add single table values to the evaluation result via intersection

        ie. for 'FROM A, B WHERE A.id = B.id and A.number > 5', the 'A.number > 5' should
        reduce the records that passed the 'A.id = B.id' condition by removing
        key value pairs from _join_map[(0,1)] where the key is for a row in A that
        is not present in table_locations (does not have 'A.number > 5')

        :param table_index:
        :param table_locations:
        :return:
        """
        join_key = (table_index, table_index + 1)
        if self._join_map[join_key] is None:
            # If there are no constraints on this index, use these locations
            self._join_map[join_key] = {}
            for loc in table_locations:
                self._join_map[join_key][loc] = None
        else:
            # Else remove constraints from the join map not found in these new values
            keys_to_keep = set(table_locations)
            for k in list(self._join_map[join_key].keys()):
                if k not in keys_to_keep:
                    del self._join_map[join_key][k]

    def intersect_consecutive_table_rows(self, first_table_index, row_map):
        assert self._tuple_length > 1, "There must be multiple tables"

        join_key = (first_table_index, first_table_index + 1)
        if self._join_map[join_key] is None:
            # There are no constraints on this index, use these
            self._join_map[join_key] = row_map
        else:
            # There are already constraints that must be intersected with the row_map
            for k in list(self._join_map[join_key].keys()):
                if k not in row_map:
                    # Each key in join_map must be in row_map and vice versa
                    del self._join_map[join_key][k]
                else:
                    # Each value a key maps to must be in join_map and vice versa
                    if self._join_map[join_key][k] is None:
                        self._join_map[join_key][k] = row_map[k]
                    else:
                        for v in list(self._join_map[join_key][k].keys()):
                            if v not in row_map[k]:
                                del self._join_map[join_key][k][v]

    def intersect_reflexive_table_rows(self, table_index, row_map):
        """
        for example,
        'FROM M WHERE M.producer = M.director'
        we can only filter rows from the same table
        :param table_index:
        :param row_map:
        :return:
        """
        # Condense the map
        for k in list(row_map.keys()):
            if row_map[k] is not None and k not in row_map[k]:
                del row_map[k]
        keys_to_keep = set(row_map.keys())

        # Compute the intersection
        join_key = (table_index, table_index + 1)
        if self._join_map[join_key] is None:
            # There are no constraints
            self._join_map[join_key] = {}
            for k in keys_to_keep:
                self._join_map[join_key][k] = None
        else:
            # Need to intersect the results with existing results
            for k in list(self._join_map[join_key].keys()):
                if k not in keys_to_keep:
                    del self._join_map[join_key][k]

    def intersect_nonconsecutive_table_rows(self, table_index1, table_index2, row_map):
        """
        For cases like the following
        "FROM A, B, C WHERE A.id = C.id", we could abstract the ordering as "FROM A, C, B ...", but
        in cases like "FROM A, B, C WHERE A.id = B.id AND A.id = C.id" we have to pipe
        the constraints through each table index from [table_index1,table_index2]

        """
        assert table_index1 < table_index2 - 1

        join_key = (table_index1, table_index1 + 1)
        if self._join_map[join_key] is None:
            self._join_map[join_key] = {}
            for k in row_map:
                self._join_map[join_key][k] = None
        else:
            # Remove elements not in intersection from consecutive map
            for k in list(self._join_map[join_key].keys()):
                if k not in row_map:
                    del self._join_map[join_key][k]

            # Remove elements not in intersection from non consecutive map
            for k in list(row_map.keys()):
                if k not in self._join_map[join_key]:
                    del row_map[k]

            # Keep the mapping for tuple generation
            self._aux_deps[(table_index1, table_index2)] = row_map
            if table_index2 in self._aux:
                self._aux[table_index2].append(table_index1)
            else:
                self._aux[table_index2] = [table_index1]

    def generate_tuples(self):
        if self._join_map[(0,1)] is None:
            return list(self.generate_tuples_recurse(None, [], 0))
        else:
            return list(self.generate_tuples_recurse(list(self._join_map[(0, 1)].keys()), [], 0))

    def generate_tuples_recurse(self, vals, acc, idx):
        if idx == self._tuple_length:
            yield tuple(acc)
        else:
            if idx in self._aux:
                # get the values of the columns that this index depends on
                deps = self._aux[idx]
                for d in deps:
                    if acc[d] in self._aux_deps[(d, idx)]:
                        if self._aux_deps[(d, idx)][acc[d]] is None:
                            vals = None
                        else:
                            if vals is None:
                                vals = self._aux_deps[(d, idx)][acc[d]]
                            else:
                                vals &= self._aux_deps[(d, idx)][acc[d]]

            if vals is None:
                pair_map = self._join_map[(idx, idx + 1)]
                if pair_map is None:
                    # use all values None
                    acc = list(acc)
                    acc.append(None)
                    yield from self.generate_tuples_recurse(None, acc, idx + 1)
                    return
                else:
                    # use the keys for the index
                    vals = list(pair_map.keys())

            acc = list(acc)
            acc.append(None)
            pairs = self._join_map[(idx, idx + 1)]

            for val in vals:
                acc[-1] = val
                if pairs is None:
                    yield from self.generate_tuples_recurse(pairs, acc, idx + 1)
                else:
                    if val in pairs:
                        yield from self.generate_tuples_recurse(pairs[val], acc, idx + 1)

    def negate(self, table_mem_locs):
        """
        Invert everything
        :param table_mem_locs:
        :return:
        """
        for k in self._join_map:
            if k == (self._tuple_length - 1, self._tuple_length):
                # The final tuple does not need to point anywhere
                final_mem_locs = set(table_mem_locs[k[0]])
                final_keys = set(self._join_map[k].keys())
                self._join_map[k] = {k1: None for k1 in (final_mem_locs - final_keys)}
            elif self._join_map[k] is None:
                # If the pair mapped all values through, don't allow any
                self._join_map[k] = {}
            else:
                # If a value was not present before, it should be now
                negated_map = {}
                for k1 in set(table_mem_locs[k[0]]) - set(self._join_map[k].keys()):
                    negated_map[k1] = None

                # Add in all the pairs that were not present before
                for k1 in self._join_map[k]:
                    if self._join_map[k][k1] is None:
                        continue
                    else:
                        difference = set(table_mem_locs[k[1]]) - self._join_map[k][k1]
                        negated_map[k1] = difference

                # Update the dict in join_map with the negation
                self._join_map[k] = negated_map

    def union(self, other):
        # Create a new EvaluationResult as the union of this result and other
        new_result = EvaluationResult(self._tuple_length)

        # Union the consecutive conditions
        for k in self._join_map:
            if self._join_map[k] is None or other._join_map[k] is None:
                # All values are accepted in one of the join maps
                new_result[k] = None

            # This pair has constraints because neither of these pairs were None
            if k not in new_result:
                new_result[k] = {}

            # Add each key and production from both
            for k1 in self._join_map[k]:
                new_result[k][k1] = self._join_map[k][k1]

            for k1 in other._join_map[k]:
                if k1 in new_result[k]:
                    if other._join_map[k][k1] is None:
                        new_result[k][k1] = None
                    elif new_result[k][k1] is None:
                        continue
                    else:
                        new_result[k][k1] |= other._join_map[k][k1]

        assert len(self._aux_deps) == 0, "Can't handle this rn"
        return new_result

    def intersect(self, other):
        new_result = EvaluationResult(self._tuple_length)

        # Intersect consecutive conditions
        for k in self._join_map:
            new_result._join_map[k] = self._join_map[k]

        for k in new_result._join_map:
            if new_result._join_map[k] is None:
                new_result._join_map[k] = other._join_map[k]
            else:
                for k1 in list(new_result._join_map[k].keys()):
                    if k1 not in other._join_map[k]:
                        del new_result._join_map[k][k1]
                    elif new_result._join_map[k][k1] is None:
                        new_result._join_map[k][k1] = other._join_map[k][k1]
                    elif other._join_map[k][k1] is not None:
                        new_result._join_map[k][k1] &= other._join_map[k][k1]

        # # Intersect the nonconsecutive conditions
        for k in self._aux_deps:
            new_result._aux_deps[k] = self._aux_deps[k]

        for k in new_result._aux_deps:
            if new_result._aux_deps[k] is None:
                new_result._aux_deps[k] = other._aux_deps[k]
            else:
                for k1 in list(new_result._aux_deps[k].keys()):
                    if k1 not in other._aux_deps[k]:
                        del new_result._aux_deps[k][k1]
                    elif new_result._aux_deps[k][k1] is None:
                        new_result._aux_deps[k][k1] = other._aux_deps[k][k1]
                    elif other._aux_deps[k][k1] is not None:
                        new_result._aux_deps[k][k1] &= other._aux_deps[k][k1]

        # assert len(self._aux_deps) == 0, "Can't handle this rn"
        return new_result

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

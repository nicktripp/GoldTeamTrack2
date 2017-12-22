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
        return list(self.generate_tuples_recurse(list(self._join_map[(0,1)].keys()), [], 0))

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


    def negate(self, table_indices):
        pass

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

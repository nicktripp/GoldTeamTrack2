from collections import defaultdict


class ResultGenerator:

    def __init__(self, tuple_length, table_mem_locs):
        self._tuple_length = tuple_length
        self._mem_locs = table_mem_locs

        # used in tuple generation
        self._right_pairs = defaultdict(list)
        self._left_pairs = defaultdict(list)
        self._left_values = {}
        self._ors = []

        """
        If a key value pair is not present in _single_constraints or _double_constraints, then all row locations are
        valid. The key for the _single_constraints map is the index of the table (in read order defined in the 
        QueryFacade). The values for the _single_constraints map will be a generator. QueryFacade.eval_comparison will
        yield values that will be integrated on demand against following additions to _single_constraints for the same
        table. The values will not be materialized until tuple generation begins for the TableProjector. The 
        _double_constraints will operate similarly, but will have keys of table index pairs. The table index pair keys
        will map to a generator of row location pairs. Again, we will defer materialization where possible.
        """
        self._single_constraints = {}
        self._double_constraints = {}

    def _intersect_generators(self, gen1, gen2):
        """
        Materializes gen1 and gen2 into sets in order to intersect them
        A generator is returned over the elements remaining in the intersection
        :param gen1: a generator
        :param gen2: another generator
        :return: a generator over the intersection
        """
        return [_ for _ in ({_ for _ in gen1} & {_ for _ in gen2})]

    def _union_generators(self, gen1, gen2):
        """
        Materializes gen1 and gen2 into sets in order to union them
        A generator is returned over the elements remaining in the union
        :param gen1: a generator
        :param gen2: another generator
        :return: a generator over the union
        """
        return [_ for _ in ({_ for _ in gen1} | {_ for _ in gen2})]

    def _mod_constraints(self, _constraints, _func, key, generator):
        if key not in _constraints:
            # The constraints have not not been added for this key
            _constraints[key] = [_ for _ in generator]
        else:
            # Constraints have been added for the key, perform the intersection or union and assign a new generator
            _constraints[key] = _func(generator, _constraints[key])

    def _sim_mod_constraints(self, _constraints, _func, key, generator):
        if key not in _constraints:
            # The constraints have not not been added for this key
            return [_ for _ in generator]
        else:
            # Constraints have been added for the key, perform the intersection or union and assign a new generator
            return _func(generator, _constraints[key])

    def reduce_single_constraints(self, table, constraint_generator):
        self._mod_constraints(self._single_constraints, self._intersect_generators, table, constraint_generator)

    def union_single_constraints(self, table, constraint_generator):
        self._mod_constraints(self._single_constraints, self._union_generators, table, constraint_generator)

    def reduce_double_constraints(self, table_pair, constraint_generator):
        assert len(table_pair) == 2 and table_pair[0] < table_pair[1], \
            'Two table constraints must be in ascending order.'

        self._mod_constraints(self._double_constraints, self._intersect_generators, table_pair, constraint_generator)

    def union_double_constraints(self, table_pair, constraint_generator):
        assert len(table_pair) == 2 and table_pair[0] < table_pair[1], \
            'Two table constraints must be in ascending order.'

        self._mod_constraints(self._double_constraints, self._union_generators, table_pair, constraint_generator)

    def generate_tuples(self):
        # reset these fields
        self._right_pairs = defaultdict(list)
        self._left_pairs = defaultdict(list)
        self._left_values = {}

        # capture constraints as sets
        self._prepare_constraints()

        # lookup all the dependencies that have the same left column or right column
        self._find_left_and_right_pairs()

        # Remove all possible tuples for the same right table index that do not support the same row location
        self._prepare_supported_constraints()

        # Make a map for all of the left values to the right values they match
        self._prepare_pairs()

        # Iterate through all possible indices of each table, stopping when the first table has visited all indices
        # self._default_table_values = [self._table_value_generator(i) for i in range(self._tuple_length)]
        self._table_values = [self._table_value_generator(i) for i in range(self._tuple_length)]
        tup = [-1] * self._tuple_length
        yield from self._generate_helper(0, tup)

        for ored in self._ors:
            yield from ored.generate_tuples()

    def _prepare_pairs(self):
        self._pairs = {}
        for table_index in range(self._tuple_length):
            if table_index in self._left_pairs:
                # Get the valid values according to single column constraints for table_index
                left_singles = self._single_constraints[
                    table_index] if table_index in self._single_constraints else None
                if left_singles is None:
                    left_singles = set(self._mem_locs[table_index])

                # Lookup all of the left values that are supported by single column and double column constraints
                # Map the left value in one column to a set of right values for another column that are valid
                self._pairs[table_index] = defaultdict(lambda: defaultdict(set))
                for table_pair in self._left_pairs[table_index]:
                    for tup in self._double_constraints[table_pair]:
                        if tup[0] in left_singles:
                            self._pairs[table_index][tup[0]][table_pair[1]].add(tup[1])

    def _reduce_double_constraints(self):
        # Remove tuples that are not supported by single column constraints
        for table_index in self._right_pairs:
            for table_pair in self._right_pairs[table_index]:
                s0, s1 = self._single_constraints[table_pair[0]], self._single_constraints[table_pair[1]]
                d = self._double_constraints[table_pair]
                self._double_constraints[table_pair] = [_ for _ in d if _[0] in s0 and _[1] in s1]

    def _prepare_supported_constraints(self):
        # Remove double constraints tuples that don't have support from single column constraints
        self._reduce_double_constraints()

        for table_index in self._right_pairs:
            # Find all of the locations that are supported by all table_pairs
            supported = None
            for table_pair in self._right_pairs[table_index]:
                if supported is None:
                    supported = {_[1] for _ in self._double_constraints[table_pair]}
                else:
                    supported &= {_[1] for _ in self._double_constraints[table_pair]}

            # Remove all tuples that are not supported by all table_pairs
            for table_pair in self._right_pairs[table_index]:
                self._double_constraints[table_pair] = [_ for _ in self._double_constraints[table_pair] if
                                                        _[1] in supported]

    def _find_left_and_right_pairs(self):
        for table_index in range(self._tuple_length):
            for table_pair in self._double_constraints:
                if table_pair[0] == table_index:
                    self._left_pairs[table_index].append(table_pair)
                elif table_pair[1] == table_index:
                    self._right_pairs[table_index].append(table_pair)

    def _prepare_constraints(self):
        for single in self._single_constraints:
            self._single_constraints[single] = list(self._single_constraints[single])
        # set unset tables to all mem location
        for table_index in range(self._tuple_length):
            if table_index not in self._single_constraints or self._single_constraints[table_index] is None:
                self._single_constraints[table_index] = self._mem_locs[table_index]
        for double in self._double_constraints:
            self._double_constraints[double] = set(self._double_constraints[double])

    def _generate_helper(self, column_index, tup):
        if column_index == self._tuple_length:
            yield tuple(tup)
            return

        for column_value in self._table_values[column_index]:
            if column_index in self._left_pairs:
                # Update the valid table values for following columns
                before = {}
                for table_pair in self._left_pairs[column_index]:
                    before[table_pair[1]] = self._table_values[table_pair[1]]
                    self._table_values[table_pair[1]] = self._pairs[column_index][column_value][table_pair[1]]

                # Yield to recursion with different table_values
                tup[column_index] = column_value
                yield from self._generate_helper(column_index + 1, tup)

                # Put the table values back to the way they were
                for table_index in before:
                    self._table_values[table_index] = before[table_index]
            else:
                tup[column_index] = column_value
                yield from self._generate_helper(column_index + 1, tup)

    def _table_value_generator(self, table_index):
        # Get the possible values according to single column constraints
        if table_index in self._pairs:
            return set(self._pairs[table_index].keys())

        if table_index not in self._single_constraints or self._single_constraints[table_index] is None:
            return set(self._mem_locs[table_index])
        else:
            return set(self._single_constraints[table_index])

    def _iand_helper(self, other, self_single_constraints, self_double_constraints):
        for single in self_single_constraints:
            other._mod_constraints(other._single_constraints, other._intersect_generators,
                                   single, self_single_constraints[single])

        for double in self_double_constraints:
            other._mod_constraints(other._double_constraints, other._intersect_generators,
                                   double, self_double_constraints[double])

    def __and__(self, other):
        """
        Does not consider the ORed results
        :param other:
        :return:
        """
        rg = ResultGenerator(self._tuple_length, self._mem_locs)

        for single in self._single_constraints:
            rg._single_constraints[single] = self._sim_mod_constraints(other._single_constraints,
                                                                       other._intersect_generators,
                                                                       single, self._single_constraints[single])

        for single in other._single_constraints:
            rg._single_constraints[single] = self._sim_mod_constraints(self._single_constraints,
                                                                       self._intersect_generators,
                                                                       single, other._single_constraints[single])

        for double in self._double_constraints:
            rg._double_constraints[double] = self._sim_mod_constraints(other._double_constraints,
                                                                       other._intersect_generators,
                                                                       double, self._double_constraints[double])

        for double in other._double_constraints:
            rg._double_constraints[double] = self._sim_mod_constraints(self._double_constraints,
                                                                       self._intersect_generators,
                                                                       double, other._double_constraints[double])
        return rg

    def __iand__(self, other):
        assert isinstance(other, ResultGenerator)

        rg = self & other

        ors = []
        for ored in other._ors:
            # AND rg with all of the ors of other
            ors.append(self & ored)

        for ored in self._ors:
            # AND all the ORs with other and its ors
            ors.append(ored & other)
            ors.extend([ored & other_ored for other_ored in other._ors])

        self._single_constraints = rg._single_constraints
        self._double_constraints = rg._double_constraints
        self._ors = ors
        return self

    def __ior__(self, other):
        assert isinstance(other, ResultGenerator)

        # Append the list of ORed results
        ors = other._ors
        other._ors = []
        self._ors.append(other)
        self._ors.extend(ors)

        return self

    def _negate_double_generator(self, t1, t2, s):
        for v1 in self._mem_locs[t1]:
            for v2 in self._mem_locs[t2]:
                if (v1, v2) not in s:
                    yield v1, v2

    def negate(self):
        for single in self._single_constraints:
            mem_locs = self._mem_locs[single]
            diff = set(mem_locs) - set(self._single_constraints[single])
            self._single_constraints[single] = list(diff)

        for double in self._double_constraints:
            s1 = set(self._double_constraints[double])
            self._double_constraints[double] = list(self._negate_double_generator(double[0], double[1], s1))

        # !(a | b) is the equivalent of of !a & !b
        ors = self._ors
        self._ors = []  # Remove the ors from recursion

        for ored in ors:
            ored.negate()
            self &= ored

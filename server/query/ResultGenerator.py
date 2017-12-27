from collections import defaultdict


class ResultGenerator:

    def __init__(self, tuple_length, table_mem_locs):
        self._tuple_length = tuple_length
        self._mem_locs = table_mem_locs

        # used in tuple generation
        self._right_pairs = defaultdict(list)
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
        self._left_values = {}

        # capture constraints as lists
        for single in self._single_constraints:
            self._single_constraints[single] = list(self._single_constraints[single])

        for double in self._double_constraints:
            self._double_constraints[double] = list(self._double_constraints[double])

            # lookup the dependencies of each tuple
        for table_index in range(self._tuple_length):
            for table_pair in self._double_constraints:
                if table_pair[0] == table_index:
                    if table_index not in self._left_values:
                        self._left_values[table_index] = {_[0] for _ in self._double_constraints[table_pair]}
                    else:
                        self._left_values[table_index] &= {_[0] for _ in self._double_constraints[table_pair]}
                elif table_pair[1] == table_index:
                    self._right_pairs[table_index].append(table_pair)

        yield from self._generate_recurse(0, [])

        for ored in self._ors:
            yield from ored.generate_tuples()

    def _generate_recurse(self, table_index, accumulation):
        if table_index == self._tuple_length:
            # Base Case
            yield tuple(accumulation)
            return

        # Find the possible values for this table considering the single constraints
        if table_index not in self._single_constraints or self._single_constraints[table_index] is None:
            values = set(self._mem_locs[table_index])
        else:
            values = set(self._single_constraints[table_index])

        # filter values depending on previous accumulation values
        if table_index in self._right_pairs:
            for table_pair in self._right_pairs[table_index]:
                # Only pairs with accumulation[table_pair[0]] first and an item from
                # [_[1] for _ in self._double_constraints[table_pair]]) second are valid pairs
                values &= {_[1] for _ in self._double_constraints[table_pair] if _[0] == accumulation[table_pair[0]]}

        # filter values depending on following accumulation values
        if table_index in self._left_values:
            values &= set(self._left_values[table_index])

        # Add another place to the accumulation and yield over each potential value
        if len(accumulation) < table_index + 1:
            accumulation.append(-1)
        for value in values:
            accumulation[table_index] = value
            yield from self._generate_recurse(table_index + 1, accumulation)

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
            self._single_constraints[single] = (_ for _ in diff)

        for double in self._double_constraints:
            s1 = set(self._double_constraints[double])
            self._double_constraints[double] = self._negate_double_generator(double[0], double[1], s1)

        # !(a | b) is the equivalent of of !a & !b
        ors = self._ors
        self._ors = [] # Remove the ors from recursion

        for ored in ors:
            ored.negate()
            self &= ored
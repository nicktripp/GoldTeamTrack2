from server.query.EvaluationResult import EvaluationResult

if __name__ == "__main__":
    er = EvaluationResult(1)
    er.intersect_row_list(0, [1, 2, 3, 4])

    assert list(er._join_map.keys()) == [(0, 1)]
    assert er._join_map[(0, 1)] == {1: None, 2: None, 3: None, 4: None}
    assert er.generate_tuples() == [(1,), (2,), (3,), (4,)]

    er = EvaluationResult(2)
    er.intersect_row_list(0, [1, 2, 3])
    er.intersect_row_list(1, [4, 5, 6])

    assert list(er._join_map.keys()) == [(0, 1), (1, 2)]
    assert er._join_map[(0, 1)] == {1: None, 2: None, 3: None}
    assert er._join_map[(1, 2)] == {4: None, 5: None, 6: None}
    assert er.generate_tuples() == [(1, 4), (1, 5), (1, 6), (2, 4), (2, 5), (2, 6), (3, 4), (3, 5), (3, 6)]

    er = EvaluationResult(2)
    er.intersect_row_list(0, [1, 2, 3])
    er.intersect_consecutive_table_rows(0, {0: {1, 2, 3}, 1: None, 2: {1, 2, 3}})
    assert er._join_map[(0, 1)] == {1: None, 2: {1, 2, 3}}
    assert er._join_map[(1, 2)] is None
    assert er.generate_tuples() == [(1, None), (2, 1), (2, 2), (2, 3)]

    er = EvaluationResult(1)
    er.intersect_reflexive_table_rows(0, {0: {1, 2}, 1: None, 2: {2, 3}})
    assert er._join_map[(0, 1)] == {1: None, 2: None}
    assert er.generate_tuples() == [(1,), (2,)]

    er = EvaluationResult(2)
    er.intersect_consecutive_table_rows(0, {0: {1, 2, 3}, 1: None, 2: {1, 2, 3}})
    er.intersect_reflexive_table_rows(0, {0: {1, 2}, 1: None, 2: {2, 3}})
    assert er._join_map[(0, 1)] == {1: None, 2: {1, 2, 3}}
    assert er.generate_tuples() == [(1, None), (2, 1), (2, 2), (2, 3)]

    er = EvaluationResult(3)
    er.intersect_row_list(0, [1, 2])
    er.intersect_consecutive_table_rows(0, {0: {1, 2, 3}, 1: None, 2: {1, 2}})
    assert er._join_map[(0, 1)] == {1: None, 2: {1, 2}}
    assert er.generate_tuples() == [(1, None, None), (2, 1, None), (2, 2, None)]

    er.intersect_nonconsecutive_table_rows(0, 2, {4: {4, 5, 6}, 2: {4, 5}})
    assert er._join_map[(0, 1)] == {2: {1, 2}}
    assert er._join_map[(1, 2)] is None
    assert er._aux_deps[(0, 2)] == {2: {4, 5}}
    assert er.generate_tuples() == [(2, 1, 4), (2, 1, 5), (2, 2, 4), (2, 2, 5)]

    ############################
    er = EvaluationResult(3)
    er.intersect_row_list(0, [1, 2])
    er.intersect_consecutive_table_rows(0, {0: {1, 2, 3}, 1: None, 2: {1, 2, 3}})
    assert er._join_map[(0, 1)] == {1: None, 2: {1, 2, 3}}
    assert er.generate_tuples() == [(1, None, None), (2, 1, None), (2, 2, None), (2, 3, None)]

    er.intersect_nonconsecutive_table_rows(0, 2, {4: {4, 5, 6}, 2: {4, 5}})
    er.intersect_row_list(1, [1, 2, 3])
    assert er._join_map[(0, 1)] == {2: {1, 2, 3}}
    assert er._join_map[(1, 2)] == {1: None, 2: None, 3: None}
    assert er._aux_deps[(0, 2)] == {2: {4, 5}}
    assert er.generate_tuples() == [(2, 1, 4), (2, 1, 5), (2, 2, 4), (2, 2, 5), (2, 3, 4), (2, 3, 5)]


    ############################
    er = EvaluationResult(3)
    er.intersect_row_list(0, [1, 2])
    er.intersect_consecutive_table_rows(0, {0: {1, 2, 3}, 1: None, 2: {1, 2, 3}})
    assert er._join_map[(0, 1)] == {1: None, 2: {1, 2, 3}}
    assert er.generate_tuples() == [(1, None, None), (2, 1, None), (2, 2, None), (2, 3, None)]

    er.intersect_nonconsecutive_table_rows(0, 2, {4: {4, 5, 6}, 2: {4, 5}})
    er.intersect_row_list(1, [1, 2])
    assert er._join_map[(0, 1)] == {2: {1, 2, 3}}
    assert er._join_map[(1, 2)] == {1: None, 2: None}
    assert er._aux_deps[(0, 2)] == {2: {4, 5}}
    assert er.generate_tuples() == [(2, 1, 4), (2, 1, 5), (2, 2, 4), (2, 2, 5)]

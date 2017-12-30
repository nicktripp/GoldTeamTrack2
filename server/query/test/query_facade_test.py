from server.Hangman import Hangman
from server.indexing.BTreeIndex import BTreeIndex
from server.indexing.BTreeIndex import BTreeIndex

if __name__ == "__main__":
    query = "SELECT S.a FROM small S"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1', '5', '9'}

    query = "SELECT S.a FROM small S"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1', '5', '9'}

    query = "SELECT S.b * 3 FROM small S WHERE S.c / 2 < 4"
    out = Hangman.execute(query, BTreeIndex)
    print(out)
    assert set(out) == {'6.0', '18.0'}

    query = "SELECT S.b / 2 FROM small S WHERE S.c / 2 < 4"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1.0', '3.0'}

    query = "SELECT S.b + 3 FROM small S WHERE S.c / 2 < 4"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'5.0', '9.0'}

    query = "SELECT S.b - 3 FROM small S WHERE S.c / 2 < 4"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'-1.0', '3.0'}

    query = "SELECT S.* FROM small S"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4', '5,6,7,8', '9,10,11,12'}

    query = "SELECT * FROM small S"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4', '5,6,7,8', '9,10,11,12'}

    query = "SELECT * FROM small S WHERE S.a > 1"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'5,6,7,8', '9,10,11,12'}

    query = "SELECT * FROM small S WHERE (S.a > 1)"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'5,6,7,8', '9,10,11,12'}

    query = "SELECT * FROM small S WHERE NOT (S.a > 1)"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4'}

    query = "SELECT * FROM small S WHERE NOT S.a > 1"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4'}

    query = "SELECT * FROM small S WHERE NOT (S.a = 1 OR S.a = 5)"
    out = Hangman.execute(query, BTreeIndex)
    print(out)
    assert set(out) == {'9,10,11,12'}

    query = "SELECT * FROM small S WHERE S.b > 2 AND (NOT (S.a = 5))"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'9,10,11,12'}

    query = 'SELECT * FROM small_text S WHERE NOT S.a LIKE "%ac%"'
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {"abc,abd,abe,abf,abg"}

    query = 'SELECT * FROM small_text S WHERE NOT (S.a LIKE "%ac%")'
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {"abc,abd,abe,abf,abg"}

    query = "SELECT * FROM small S WHERE S.a > 1 AND S.b <> 10"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'5,6,7,8'}

    query = 'SELECT * FROM small_text S WHERE S.a LIKE "ab%"'
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {"abc,abd,abe,abf,abg"}

    query = 'SELECT * FROM small_text S WHERE S.a LIKE "%ac%"'
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {"acb,adb,aeb,afb,agb", "bac,bad,bae,baf,bag"}

    query = 'SELECT * FROM small_text S WHERE S.a LIKE "ab%" OR S.a LIKE "%ac%"'
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {"abc,abd,abe,abf,abg", "acb,adb,aeb,afb,agb", "bac,bad,bae,baf,bag"}

    query = 'SELECT * FROM small_text S WHERE S.a LIKE "%a%" AND S.b = "abd"'
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {"abc,abd,abe,abf,abg"}

    query = "SELECT S1.* FROM small S1, small S2 WHERE S1.a > 1 AND S2.a < 9"
    out = Hangman.execute(query, BTreeIndex)
    print(out)
    assert set(out) == {'5,6,7,8', '5,6,7,8', '9,10,11,12', '9,10,11,12'}

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a > 1 AND S2.a < 9"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'5,6,7,8,1,2,3,4', '5,6,7,8,5,6,7,8', '9,10,11,12,1,2,3,4', '9,10,11,12,5,6,7,8'}

    query = "SELECT * FROM small S1 WHERE S1.a = 1 OR S1.a <> 1"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4', '5,6,7,8', '9,10,11,12'}

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a = 1"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,1,2,3,4',
                        '1,2,3,4,5,6,7,8',
                        '1,2,3,4,9,10,11,12'}

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a = 1 OR S1.a = 2"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,1,2,3,4',
                        '1,2,3,4,5,6,7,8',
                        '1,2,3,4,9,10,11,12'}

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a = 1 OR S1.a = 5"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,1,2,3,4',
                        '1,2,3,4,5,6,7,8',
                        '1,2,3,4,9,10,11,12',
                        '5,6,7,8,1,2,3,4',
                        '5,6,7,8,5,6,7,8',
                        '5,6,7,8,9,10,11,12'}

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a = 1 OR (S1.a = 5)"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,1,2,3,4',
                        '1,2,3,4,5,6,7,8',
                        '1,2,3,4,9,10,11,12',
                        '5,6,7,8,1,2,3,4',
                        '5,6,7,8,5,6,7,8',
                        '5,6,7,8,9,10,11,12'}

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a = 1 OR (S1.a = 5 OR S1.a = 9)"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,1,2,3,4', '1,2,3,4,5,6,7,8', '1,2,3,4,9,10,11,12', '5,6,7,8,1,2,3,4',
                        '5,6,7,8,5,6,7,8', '5,6,7,8,9,10,11,12', '9,10,11,12,1,2,3,4', '9,10,11,12,5,6,7,8',
                        '9,10,11,12,9,10,11,12'}

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE (S1.a = 1 OR (S1.a = 5 OR S1.a = 9)) AND NOT S1.a = 1"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'5,6,7,8,1,2,3,4',
                        '5,6,7,8,5,6,7,8',
                        '5,6,7,8,9,10,11,12',
                        '9,10,11,12,1,2,3,4',
                        '9,10,11,12,5,6,7,8',
                        '9,10,11,12,9,10,11,12'}

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE (S2.b = 2 AND (S1.a < 9 AND S1.a > 1))"
    out = Hangman.execute(query, BTreeIndex)
    print(out)
    assert set(out) == {'5,6,7,8,1,2,3,4'}

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE (S2.b = 2 AND (S1.a < 9 AND S1.a > 1 OR S1.a <> 5))"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,1,2,3,4',
                        '5,6,7,8,1,2,3,4',
                        '9,10,11,12,1,2,3,4'}

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE (S2.b = 2 AND (S1.a < 9 AND S1.a > 1 OR S1.a <> 5))"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,1,2,3,4',
                        '5,6,7,8,1,2,3,4',
                        '9,10,11,12,1,2,3,4'}

    query = "SELECT S.*, M.* FROM small S, medium M WHERE S.a = M.a"
    out = Hangman.execute(query, BTreeIndex)
    print(out)
    assert set(out) == {'1,2,3,4,1,2,3', '1,2,3,4,1,4,5', '1,2,3,4,1,6,7'}

    query = "SELECT S.*, M.* FROM small S, medium M WHERE M.a <> 1"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,2,5,6', '5,6,7,8,2,5,6', '9,10,11,12,2,5,6', '1,2,3,4,3,4,5', '5,6,7,8,3,4,5',
                        '9,10,11,12,3,4,5', '1,2,3,4,3,8,9', '5,6,7,8,3,8,9', '9,10,11,12,3,8,9', '1,2,3,4,2,3,4',
                        '5,6,7,8,2,3,4', '9,10,11,12,2,3,4', '1,2,3,4,2,7,8', '5,6,7,8,2,7,8', '9,10,11,12,2,7,8',
                        '1,2,3,4,3,6,7', '5,6,7,8,3,6,7', '9,10,11,12,3,6,7'}

    query = "SELECT S.*, M.* FROM small S, medium M WHERE S.a = M.a OR M.a <> 1"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,2,5,6', '5,6,7,8,2,5,6', '9,10,11,12,2,5,6', '1,2,3,4,3,4,5', '5,6,7,8,3,4,5',
                        '9,10,11,12,3,4,5', '1,2,3,4,3,8,9', '5,6,7,8,3,8,9', '9,10,11,12,3,8,9', '1,2,3,4,2,3,4',
                        '5,6,7,8,2,3,4', '9,10,11,12,2,3,4', '1,2,3,4,2,7,8', '5,6,7,8,2,7,8', '9,10,11,12,2,7,8',
                        '1,2,3,4,3,6,7', '5,6,7,8,3,6,7', '9,10,11,12,3,6,7', '1,2,3,4,1,2,3', '1,2,3,4,1,4,5',
                        '1,2,3,4,1,6,7'}

    query = "SELECT S.*, M.* FROM small S, medium M WHERE S.a = M.a AND M.a < 2"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,1,2,3', '1,2,3,4,1,4,5', '1,2,3,4,1,6,7'}

    query = "SELECT S.*, M.* FROM small S, medium M WHERE S.a = M.a AND M.a <> 1"
    out = Hangman.execute(query, BTreeIndex)
    print(out)
    assert set(out) == set()

    query = "SELECT S.*, M.* FROM small S JOIN medium M"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,1,2,3', '1,2,3,4,1,4,5', '1,2,3,4,1,6,7', '1,2,3,4,2,3,4', '1,2,3,4,2,5,6',
                        '1,2,3,4,2,7,8', '1,2,3,4,3,4,5', '1,2,3,4,3,6,7', '1,2,3,4,3,8,9', '5,6,7,8,1,2,3',
                        '5,6,7,8,1,4,5', '5,6,7,8,1,6,7', '5,6,7,8,2,3,4', '5,6,7,8,2,5,6', '5,6,7,8,2,7,8',
                        '5,6,7,8,3,4,5', '5,6,7,8,3,6,7', '5,6,7,8,3,8,9', '9,10,11,12,1,2,3', '9,10,11,12,1,4,5',
                        '9,10,11,12,1,6,7', '9,10,11,12,2,3,4', '9,10,11,12,2,5,6', '9,10,11,12,2,7,8',
                        '9,10,11,12,3,4,5', '9,10,11,12,3,6,7', '9,10,11,12,3,8,9'}

    query = "SELECT S.*, M.* FROM small S JOIN medium M ON (S.a = M.a)"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,1,2,3', '1,2,3,4,1,6,7', '1,2,3,4,1,4,5'}

    query = "SELECT S.*, S1.b, M.* FROM small S JOIN medium M JOIN small S1 ON (S.a = M.a AND S.b = S1.b)"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,2,1,2,3', '1,2,3,4,2,1,6,7', '1,2,3,4,2,1,4,5'}

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a = S2.a - 8"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,9,10,11,12'}, print(out)

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a = S2.a - 4"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,5,6,7,8', '5,6,7,8,9,10,11,12'}

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a + 4 = S2.a"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,5,6,7,8', '5,6,7,8,9,10,11,12'}

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a + 4 = S2.a - 4"
    out = Hangman.execute(query, BTreeIndex)
    assert set(out) == {'1,2,3,4,9,10,11,12'}

    # TODO support column + column arithmetic?
    # query = "SELECT S1.a FROM small S1 WHERE S1.a + S1.a = S1.a * 2"
    # out = Hangman.execute(query, BTreeIndex)
    # assert set(out) == {'1', '5', '9'}
    #
    # query = "SELECT S1.a FROM small S1 WHERE S1.a + S1.a = 2 * S1.a"
    # out = Hangman.execute(query, BTreeIndex)
    # assert set(out) == {'1', '5', '9'}


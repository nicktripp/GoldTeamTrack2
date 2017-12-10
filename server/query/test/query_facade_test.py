from server.Hangman import Hangman

if __name__ == "__main__":
    query = "SELECT S.a FROM small S"
    out = Hangman.execute(query)
    assert out == ['1', '5', '9']

    query = "SELECT S.a, S.b FROM small S"
    out = Hangman.execute(query)
    assert out == ['1,2', '5,6', '9,10']

    query = "SELECT S.* FROM small S"
    out = Hangman.execute(query)
    assert out == ['1,2,3,4', '5,6,7,8', '9,10,11,12']

    query = "SELECT * FROM small S"
    out = Hangman.execute(query)
    assert out == ['1,2,3,4', '5,6,7,8', '9,10,11,12']

    query = "SELECT * FROM small S WHERE S.a > 1"
    out = Hangman.execute(query)
    assert out == ['5,6,7,8', '9,10,11,12']

    query = "SELECT * FROM small S WHERE S.a > 1 AND S.b <> 10"
    out = Hangman.execute(query)
    assert out == ['5,6,7,8']

    query = 'SELECT * FROM small_text S WHERE S.a LIKE "ab%"'
    out = Hangman.execute(query)
    assert out == ["abc,abd,abe,abf,abg"]

    query = 'SELECT * FROM small_text S WHERE S.a LIKE "%ac%"'
    out = Hangman.execute(query)
    assert out == ["acb,adb,aeb,afb,agb", "bac,bad,bae,baf,bag"]

    query = 'SELECT * FROM small_text S WHERE S.a LIKE "ab%" OR S.a LIKE "%ac%"'
    out = Hangman.execute(query)
    assert out == ["abc,abd,abe,abf,abg", "acb,adb,aeb,afb,agb", "bac,bad,bae,baf,bag"]

    query = 'SELECT * FROM small_text S WHERE S.a LIKE "%a%" AND S.b = "abd"'
    out = Hangman.execute(query)
    assert out == ["abc,abd,abe,abf,abg"]

    query = "SELECT S1.* FROM small S1, small S2 WHERE S1.a > 1 AND S2.a < 9"
    out = Hangman.execute(query)
    assert out == ['5,6,7,8', '5,6,7,8', '9,10,11,12', '9,10,11,12']

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a > 1 AND S2.a < 9"
    out = Hangman.execute(query)
    assert out == ['5,6,7,8,1,2,3,4', '5,6,7,8,5,6,7,8', '9,10,11,12,1,2,3,4', '9,10,11,12,5,6,7,8']

    query = "SELECT * FROM small S1 WHERE S1.a = 1 OR S1.a <> 1"
    out = Hangman.execute(query)
    assert out == ['1,2,3,4', '5,6,7,8', '9,10,11,12']

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a = 1"
    out = Hangman.execute(query)
    assert out == ['1,2,3,4,1,2,3,4',
                   '1,2,3,4,5,6,7,8',
                   '1,2,3,4,9,10,11,12']

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a = 1 OR S1.a = 2"
    out = Hangman.execute(query)
    assert out == ['1,2,3,4,1,2,3,4',
                   '1,2,3,4,5,6,7,8',
                   '1,2,3,4,9,10,11,12']

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a = 1 OR S1.a = 5"
    out = Hangman.execute(query)
    assert out == ['1,2,3,4,1,2,3,4',
                   '1,2,3,4,5,6,7,8',
                   '1,2,3,4,9,10,11,12',
                   '5,6,7,8,1,2,3,4',
                   '5,6,7,8,5,6,7,8',
                   '5,6,7,8,9,10,11,12']

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a = 1 OR (S1.a = 5)"
    out = Hangman.execute(query)
    assert out == ['1,2,3,4,1,2,3,4',
                   '1,2,3,4,5,6,7,8',
                   '1,2,3,4,9,10,11,12',
                   '5,6,7,8,1,2,3,4',
                   '5,6,7,8,5,6,7,8',
                   '5,6,7,8,9,10,11,12']

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE S1.a = 1 OR (S1.a = 5 OR S1.a = 9)"
    out = Hangman.execute(query)
    assert out == ['1,2,3,4,1,2,3,4',
                   '1,2,3,4,5,6,7,8',
                   '1,2,3,4,9,10,11,12',
                   '5,6,7,8,1,2,3,4',
                   '5,6,7,8,5,6,7,8',
                   '5,6,7,8,9,10,11,12',
                   '9,10,11,12,1,2,3,4',
                   '9,10,11,12,5,6,7,8',
                   '9,10,11,12,9,10,11,12']

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE (S2.b = 2 AND (S1.a < 9 AND S1.a > 1))"
    out = Hangman.execute(query)
    assert out == ['5,6,7,8,1,2,3,4']

    query = "SELECT S1.*, S2.* FROM small S1, small S2 WHERE (S2.b = 2 AND (S1.a < 9 AND S1.a > 1 OR S1.a <> 5))"
    out = Hangman.execute(query)
    assert out == ['1,2,3,4,1,2,3,4',
                   '5,6,7,8,1,2,3,4',
                   '9,10,11,12,1,2,3,4']

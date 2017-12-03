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
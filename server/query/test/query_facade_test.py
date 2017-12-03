from server.Hangman import Hangman

if __name__ == "__main__":
    query = "SELECT S.a FROM small S"
    out = Hangman.execute(query)
    assert out == ['1', '5', '9']

    query = "SELECT S.a, S.b FROM small S"
    out = Hangman.execute(query)
    assert out == ['1,2', '5,6', '9,10']
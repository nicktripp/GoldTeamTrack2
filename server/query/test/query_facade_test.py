from server.Hangman import Hangman

if __name__ == "__main__":
    query = "SELECT S.a FROM small S"
    out = Hangman.execute(query)
    print(out)
    assert out == ['1', '5', '9']
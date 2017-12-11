from server.Hangman import Hangman

if __name__ == "__main__":

    query = 'SELECT R.review_id, R.stars, R.useful FROM review50k R WHERE R.stars >= 4 AND R.useful > 20'
    out = Hangman.execute(query)
    print(out)
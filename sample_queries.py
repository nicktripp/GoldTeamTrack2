from server.Hangman import Hangman
import time

if __name__ == "__main__":
    t0 = time.time()
    query = 'SELECT R.review_id, R.stars, R.useful FROM review50k R WHERE R.stars >= 4 AND R.useful > 20'
    out = Hangman.execute(query)
    t1 = time.time()
    print("Time Elapsed %f s over 50k" % (t1 - t0))
    print(out)

    t0 = time.time()
    query = 'SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful FROM business B JOIN review50k R ON (' \
            'B.business_id = R.business_id) WHERE B.city = "Champaign" AND B.state = "IL" '
    out = Hangman.execute(query)
    t1 = time.time()
    print("Time Elapsed %f s over 50k" % (t1 - t0))
    print(out)

    t0 = time.time()
    query = 'SELECT DISTINCT B.name FROM business B JOIN review50k JOIN photos P ON (B.business_id = R.business_id ' \
            'AND B.business_id = P.business_id) WHERE B.city = "Champaign" AND B.state = "IL" AND R.stars = 5 AND ' \
            'P.label = "inside" '
    out = Hangman.execute(query)
    t1 = time.time()
    print("Time Elapsed %f s over 50k" % (t1 - t0))


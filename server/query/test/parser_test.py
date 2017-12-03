from server.query.Column import Column
from server.query.Parser import Parser
from server.query.Table import Table
from server.query.SQLParsingError import SQLParsingError

if __name__ == "__main__":
    query = "SELECT * FROM movies"
    p = Parser(query)
    cols, tbls, conds, f_dist = p.parse_select_from_where()
    assert cols == ['*']
    assert tbls == [Table('movies')]
    assert conds == []
    assert f_dist is False

    query = "SELECT DISTINCT * FROM movies M1, movies M2"
    p = Parser(query)
    cols, tbls, conds, f_dist = p.parse_select_from_where()
    assert cols == ['*']
    assert tbls == [Table('movies', 'M1'), Table('movies', 'M2')]
    assert conds == []
    assert f_dist is True

    query = 'SELECT movie_title FROM movies WHERE movie_title = "Spider-Man 3"'
    p = Parser(query)
    cols, tbls, conds, _ = p.parse_select_from_where()
    assert cols == [Column(Table('movies'),'movie_title')]
    assert tbls == [Table('movies')]
    assert str(conds[0][0]) == 'movie_title = "Spider-Man 3"'

    query = 'SELECT M1.movie_title FROM movies M1 WHERE M1.movie_title = "Spider-Man 3"'
    p = Parser(query)
    cols, tbls, conds, _ = p.parse_select_from_where()
    assert cols == [Column(Table('movies', 'M1'), 'movie_title')]
    assert tbls == [Table('movies', 'M1')]
    assert str(conds[0][0]) == 'M1.movie_title = "Spider-Man 3"'

    query = 'SELECT M1.movie_title, M2.movie_title FROM movies M1, movies M2 WHERE M1.movie_title = "Spider-Man 3"'
    p = Parser(query)
    cols, tbls, conds, _ = p.parse_select_from_where()
    assert cols == [Column(Table('movies', 'M1'), 'movie_title'), Column(Table('movies', 'M2'), 'movie_title')]
    assert tbls == [Table('movies', 'M1'), Table('movies', 'M2')]
    assert str(conds[0][0]) == 'M1.movie_title = "Spider-Man 3"'

    query = 'SELECT movie_title FROM movies WHERE movie_title = "Spider-Man 3" AND release_year < 2010'
    p = Parser(query)
    cols, tbls, conds, _ = p.parse_select_from_where()
    assert cols == [Column(Table('movies'),'movie_title')]
    assert tbls == [Table('movies')]
    assert str(conds[0][0]) == 'movie_title = "Spider-Man 3"'
    assert str(conds[0][1]) == 'release_year < 2010'

    query = 'SELECT movie_title FROM movies WHERE movie_title = "Spider-Man 3" AND release_year < 2010 OR release_year = 2011'
    p = Parser(query)
    cols, tbls, conds, _ = p.parse_select_from_where()
    assert cols == [Column(Table('movies'),'movie_title')]
    assert tbls == [Table('movies')]
    assert str(conds[0][0]) == 'movie_title = "Spider-Man 3"'
    assert str(conds[0][1]) == 'release_year < 2010'
    assert str(conds[1][0]) == 'release_year = 2011'

    query = 'SELECT movie_title FROM movies WHERE movie_title = "Spider-Man 3" AND (release_year < 2010 OR release_year = 2011)'
    p = Parser(query)
    cols, tbls, conds, _ = p.parse_select_from_where()
    assert cols == [Column(Table('movies'),'movie_title')]
    assert tbls == [Table('movies')]
    assert str(conds[0][0]) == 'movie_title = "Spider-Man 3"'
    assert str(conds[0][1][0][0]) == 'release_year < 2010'
    assert str(conds[0][1][1][0]) == 'release_year = 2011'

    query = 'SELECT movie_title FROM movies WHERE (movie_title = "Spider-Man 3" AND (release_year < 2010 OR release_year = 2011))'
    p = Parser(query)
    cols, tbls, conds, _ = p.parse_select_from_where()
    assert cols == [Column(Table('movies'),'movie_title')]
    assert tbls == [Table('movies')]
    assert str(conds[0][0][0][0]) == 'movie_title = "Spider-Man 3"'
    assert str(conds[0][0][0][1][0][0]) == 'release_year < 2010'
    assert str(conds[0][0][0][1][1][0]) == 'release_year = 2011'

    query = 'SELECT DISTINCT M1.movie_title, M2.movie_title FROM movies M1 JOIN movies M2 ON (M1.title = M2.title AND M1.score = M2.score) WHERE M1.title > M2.title'
    p = Parser(query)
    cols, tbls, conds, f_dist = p.parse_select_from_where()
    assert f_dist is True
    assert cols == [Column(Table('movies', 'M1'), 'movie_title'), Column(Table('movies', 'M2'), 'movie_title')]
    assert tbls == [Table('movies', 'M1'), Table('movies', 'M2')]
    assert str(conds[0][0][0][0]) == "M1.title = M2.title"
    assert str(conds[0][0][0][1]) == "M1.score = M2.score"
    assert str(conds[0][1]) == "M1.title > M2.title"

    query = 'SELECT  M1.movie_title FROM movies M1 JOIN movies M2 JOIN movies M3 ON (M1.title = M2.title AND M1.score = M3.score)'
    p = Parser(query)
    cols, tbls, conds, f_dist = p.parse_select_from_where()
    assert f_dist is False
    assert cols == [Column(Table('movies', 'M1'), 'movie_title')]
    assert tbls == [Table('movies', 'M1'), Table('movies', 'M2'), Table('movies', 'M3')]
    assert str(conds[0][0]) == "M1.title = M2.title"
    assert str(conds[0][1]) == "M1.score = M3.score"

    query = 'SELECT M.movie_title FROM movies M1 JOIN Movies M2 ON (M1.title > M2.title)'
    threw_err = False
    try:
        p = Parser(query)
        cols, tbls, conds, _ = p.parse_select_from_where()
    except SQLParsingError as err:
        assert err.message == "JOIN conditions must be equality comparisons!"
        threw_err = True
    assert threw_err is True

    # The exact example queries given by the Prof

    #SELECT R.review_id, R.stars, R.useful FROM review-1m.csv R WHERE R.stars >= 4 AND R.useful > 20
    query = "SELECT R.review_id, R.stars, R.useful FROM review-1m R WHERE R.stars >= 4 AND R.useful > 20"
    p = Parser(query)
    cols, tbls, conds, DISTINCT = p.parse_select_from_where()
    assert DISTINCT is False
    assert cols == [Column(Table('review-1m', 'R'), 'review_id'), Column(Table('review-1m', 'R'), 'stars'), Column(Table('review-1m', 'R'), 'useful')]
    assert tbls == [Table('review-1m', 'R')]
    assert str(conds[0][0]) == 'R.stars >= 4'
    assert str(conds[0][1]) == 'R.useful > 20'

    #SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful FROM business.csv B JOIN review-1m.csv R ON (B.business_id = R.business_id) WHERE B.city = 'Champaign' AND B.state = 'IL'
    query = 'SELECT B.name, B.postal_code, R.review_id, R.stars, R.useful FROM business B JOIN review-1m R ON (B.business_id = R.business_id) WHERE B.city = "Champaign" AND B.state = "IL"'
    p = Parser(query)
    cols, tbls, conds, DISTINCT = p.parse_select_from_where()
    assert DISTINCT is False
    assert cols == [Column(Table('business', 'B'), 'name'), Column(Table('business', 'B'), 'postal_code'),
                    Column(Table('review-1m', 'R'), 'review_id'), Column(Table('review-1m', 'R'), 'stars'),
                    Column(Table('review-1m', 'R'), 'useful')]
    assert tbls == [Table('business', 'B'), Table('review-1m', 'R')]
    assert str(conds[0][0][0][0]) == "B.business_id = R.business_id"
    assert str(conds[0][1]) == 'B.city = "Champaign"'
    assert str(conds[0][2]) == 'B.state = "IL"'

    #SELECT DISTINCT B.name FROM business.csv B JOIN review-1m.csv R JOIN photos.csv P ON (B.business_id = R.business_id AND B.business_id = P.business_id) WHERE B.city = 'Champaign' AND B.state = 'IL' AND R.stars = 5 AND P.label = 'inside'
    query = 'SELECT DISTINCT B.name FROM business B JOIN review-1m R JOIN photos P ON (B.business_id = R.business_id AND B.business_id = P.business_id) WHERE B.city = "Champaign" AND B.state = "IL" AND R.stars = 5 AND P.label = "inside"'
    p = Parser(query)
    cols, tbls, conds, DISTINCT = p.parse_select_from_where()
    assert DISTINCT is True
    assert cols == [Column(Table('business', 'B'), 'name')]
    assert tbls == [Table('business', 'B'), Table('review-1m', 'R'), Table('photos', 'P')]
    assert str(conds[0][0][0][0]) == 'B.business_id = R.business_id'
    assert str(conds[0][0][0][1]) == 'B.business_id = P.business_id'
    assert str(conds[0][1]) == 'B.city = "Champaign"'
    assert str(conds[0][2]) == 'B.state = "IL"'
    assert str(conds[0][3]) == 'R.stars = 5'
    assert str(conds[0][4]) == 'P.label = "inside"'

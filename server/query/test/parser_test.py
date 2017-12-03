from server.query.Parser import Parser
from server.query.Table import Table

if __name__ == "__main__":
    query = "SELECT * FROM movies"
    p = Parser(query)
    cols, tbls, conds = p.parse_select_from_where()
    assert cols == ['*']
    assert tbls == [Table('movies')]
    assert conds == []

    query = "SELECT * FROM movies M1, movies M2"
    p = Parser(query)
    cols, tbls, conds = p.parse_select_from_where()
    assert cols == ['*']
    assert tbls == [Table('movies', 'M1'), Table('movies', 'M2')]
    assert conds == []

    query = 'SELECT movie_title FROM movies WHERE movie_title = "Spider-Man 3"'
    p = Parser(query)
    cols, tbls, conds = p.parse_select_from_where()
    assert cols == ['movie_title']
    assert tbls == [Table('movies')]
    assert str(conds[0][0]) == 'movie_title = "Spider-Man 3"'
    print(conds)

    query = 'SELECT movie_title FROM movies WHERE movie_title = "Spider-Man 3" AND release_year < 2010'
    p = Parser(query)
    cols, tbls, conds = p.parse_select_from_where()
    assert cols == ['movie_title']
    assert tbls == [Table('movies')]
    assert str(conds[0][0]) == 'movie_title = "Spider-Man 3"'
    assert str(conds[0][1]) == 'release_year < 2010'

    query = 'SELECT movie_title FROM movies WHERE movie_title = "Spider-Man 3" AND release_year < 2010 OR release_year = 2011'
    p = Parser(query)
    cols, tbls, conds = p.parse_select_from_where()
    assert cols == ['movie_title']
    assert tbls == [Table('movies')]
    assert str(conds[0][0]) == 'movie_title = "Spider-Man 3"'
    assert str(conds[0][1]) == 'release_year < 2010'
    assert str(conds[1][0]) == 'release_year = 2011'

    query = 'SELECT movie_title FROM movies WHERE movie_title = "Spider-Man 3" AND (release_year < 2010 OR release_year = 2011)'
    p = Parser(query)
    cols, tbls, conds = p.parse_select_from_where()
    assert cols == ['movie_title']
    assert tbls == [Table('movies')]
    assert str(conds[0][0]) == 'movie_title = "Spider-Man 3"'
    assert str(conds[0][1][0][0]) == 'release_year < 2010'
    assert str(conds[0][1][1][0]) == 'release_year = 2011'

    query = 'SELECT movie_title FROM movies WHERE (movie_title = "Spider-Man 3" AND (release_year < 2010 OR release_year = 2011))'
    p = Parser(query)
    cols, tbls, conds = p.parse_select_from_where()
    assert cols == ['movie_title']
    assert tbls == [Table('movies')]
    assert str(conds[0][0][0][0]) == 'movie_title = "Spider-Man 3"'
    assert str(conds[0][0][0][1][0][0]) == 'release_year < 2010'
    assert str(conds[0][0][0][1][1][0]) == 'release_year = 2011'




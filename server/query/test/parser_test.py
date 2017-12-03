from server.query.Column import Column
from server.query.Parser import Parser
from server.query.Table import Table

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
    cols, tbls, conds = p.parse_select_from_where()
    assert cols == [Column(Table('movies', 'M1'), 'movie_title')]
    assert tbls == [Table('movies', 'M1')]
    assert str(conds[0][0]) == 'M1.movie_title = "Spider-Man 3"'

    query = 'SELECT M1.movie_title, M2.movie_title FROM movies M1, movies M2 WHERE M1.movie_title = "Spider-Man 3"'
    p = Parser(query)
    cols, tbls, conds = p.parse_select_from_where()
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




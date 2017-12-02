from server.query.Parser import Parser

if __name__ == "__main__":
    query = "SELECT * FROM movies"
    p = Parser(query)
    cols, tbls, conds = p.parse_select_from_where()
    assert cols == ['*']
    assert tbls == ['movies']
    assert conds == []

    query = 'SELECT movie_title FROM movies WHERE movie_title = "Spider-Man 3"'
    p = Parser(query)
    cols, tbls, conds = p.parse_select_from_where()
    assert cols == ['movie_title']
    assert tbls == ['movies']
    print(conds)
    



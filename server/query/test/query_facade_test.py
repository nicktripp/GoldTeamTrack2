from server.indexing.FileIndexer import FileIndexer
from server.query.QueryFacade import QueryFacade
from server.query.Parser import Parser

if __name__ == "__main__":
    relative_path = '../../../'
    # FileIndexer(relative_path, 'movies', True)
    parser = Parser('SELECT movies.movie_title FROM movies WHERE movies.movie_title = "Spider-Man 3"')
    stmt = parser.parse_select_from_where()
    print(stmt)

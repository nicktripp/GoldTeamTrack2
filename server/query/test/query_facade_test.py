from server.query.QueryFacade import QueryFacade
from server.query.Parser import Parser

if __name__ == "__main__":
    relative_path = '../../../'
    # QueryFacade.prepare(relative_path)
    parser = Parser('SELECT movies.movie_title FROM "./movies.csv" WHERE movies.movie_title="Spider-Man 3"')
    stmt = parser.parse_select_from_where()
    print(stmt)

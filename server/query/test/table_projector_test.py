from server.query.Column import Column
from server.query.Table import Table
from server.query.TableProjector import TableProjector

if __name__ == "__main__":
    t1 = Table("movies")
    t2 = Table("movies")
    tables = [t1, t2]
    projection_columns = [Column(t1, 'movie_title'), Column(t2, 'director_name')]
    tp = TableProjector(tables, projection_columns)
    output = tp.aggregate([(419, 419), (710, 710), (1055, 1055)], False)
    print(output)
    assert output == ['Avatar,James Cameron', "Pirates of the Caribbean: At World's End,Gore Verbinski", 'Spectre,Sam Mendes']

    output = tp.aggregate([(419, 419), (710, 710), (1055, 1055), (419, 419)], False)
    assert output == ['Avatar,James Cameron', "Pirates of the Caribbean: At World's End,Gore Verbinski", 'Spectre,Sam Mendes', 'Avatar,James Cameron']

    output = tp.aggregate([(419, 419), (710, 710), (1055, 1055), (419, 419)], True)
    # Using DISTINCT changes the order of the output rows because of sets
    assert set(output) == set(['Avatar,James Cameron', "Pirates of the Caribbean: At World's End,Gore Verbinski", 'Spectre,Sam Mendes'])

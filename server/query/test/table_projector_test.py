from server.query.Column import Column
from server.query.Table import Table
from server.query.TableProjector import TableProjector

if __name__ == "__main__":
    # t1 = Table("movies")
    # t2 = Table("movies")
    # tables = [t1, t2]
    # projection_columns = [Column(t1, 'movie_title'), Column(t2, 'director_name')]
    # tp = TableProjector(tables, projection_columns)
    # output = tp.aggregate([(419, 419), (710, 710), (1055, 1055)], False)
    # print(output)
    # assert output == ['Avatar,James Cameron', "Pirates of the Caribbean: At World's End,Gore Verbinski", 'Spectre,Sam Mendes']
    #
    # output = tp.aggregate([(419, 419), (710, 710), (1055, 1055), (419, 419)], False)
    # assert output == ['Avatar,James Cameron', "Pirates of the Caribbean: At World's End,Gore Verbinski", 'Spectre,Sam Mendes', 'Avatar,James Cameron']
    #
    # output = tp.aggregate([(419, 419), (710, 710), (1055, 1055), (419, 419)], True)
    # # Using DISTINCT changes the order of the output rows because of sets
    # assert set(output) == set(['Avatar,James Cameron', "Pirates of the Caribbean: At World's End,Gore Verbinski", 'Spectre,Sam Mendes'])

    row = '", 4219 Providence Rd, Ste 3",,,full_bar,,,,,,,True,"{u\'garage\': False, u\'street\': False, u\'validated\': False, ' \
          'u\'lot\': False, u\'valet\': False}",,,,,,,,,False,"{u\'dessert\': False, u\'latenight\': False, u\'lunch\': True, ' \
          'u\'dinner\': False, u\'brunch\': False, u\'breakfast\': False}",,,False,,loud,,True,casual,,False,True,2.0,True,' \
          'True,True,,True,,9joPX1laPl3p6l_VfCQJFw,"Italian,Restaurants",Charlotte,,,,,,,,0,35.1569133,-80.7946407,' \
          'il Nido Ristorante,Sherwood Forest,28211,21,4.5,NC\n'
    with open('./tmp.csv', 'w', encoding='utf8') as f:
        f.write(row)

    with open('./tmp.csv', encoding='utf8') as f:
        output = TableProjector.read_col_vals_multiline(0, f)
        print(output)

    row = '"7215 Route Transcanadienne,",,,,,,,,,,,,False,,,,,,,,,,,,,,,,,,,,,,,,,,,,Nyvu02Pv5k0LCTIny4Hyew,' \
          '"Beauty & Spas,Cosmetics & Beauty Supply,Shopping",Montréal,,,,,,,,1,45.4906196784,-73.7060168136,' \
          'Entrepot l\'Oréal,Saint-Laurent,H4T 1A2,3,3.0,QC\n'
    with open('./tmp.csv', 'w', encoding='utf8') as f:
        f.write(row)

    with open('./tmp.csv', encoding='utf8') as f:
        output = TableProjector.read_col_vals_multiline(0, f)
        print(output)


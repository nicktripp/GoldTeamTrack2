from server.indexing.BTreeIndex import BTreeIndex
import os

from server.indexing.FileIndexer import FileIndexer

if __name__ == "__main__":
    print("Testing B+ Tree Index Implementation")

    # Check that it worked
    # Read Spider-Man 3,2007,Sam Raimi,USA,English,258000000,Action|Adventure|Romance

    results = {
        'Spider-Man 3,2007,Sam Raimi,USA,English,258000000,Action|Adventure|Romance,sandman|spider man|symbiote|venom|villain,2.35,0,0,156,Color,PG-13,J.K. Simmons,24000,James Franco,11000,Kirsten Dunst,4000,46055,392,1902,383056,0,6.2,336530303,http://www.imdb.com/title/tt0413300/?ref_=fn_tt_tt_1\n',
        'Spider-Man 3,2007,Sam Raimi,USA,English,258000000,Action|Adventure|Romance,sandman|spider man|symbiote|venom|villain,2.35,0,0,156,Color,PG-13,J.K. Simmons,24000,James Franco,11000,Kirsten Dunst,4000,46055,392,1902,383071,0,6.2,336530303,http://www.imdb.com/title/tt0413300/?ref_=fn_tt_tt_1\n'
    }

    # FileIndexer('../../../', 'movies')
    movie_title_index = FileIndexer.get_indexer('../../../', 'movies', 'movie_title')
    with open('../../../data/movies.csv', 'r') as f:
        for row_loc in movie_title_index.btree['Spider-Man 3']:
            f.seek(row_loc)
            assert f.readline() in results, "There are two Spider-Man 3 entries"

    count = 0
    for k, v in movie_title_index.btree.items():
        print(k)
        count += len(v)
    print(count)

    print("//////////")

    print(movie_title_index.op('Zombieland', '='))
    print(movie_title_index.op('8 Mile', '<'))

    """
Zathura: A Space Adventure
Zero Dark Thirty
Zero Effect
Zipper
Zombie Hunter
Zombieland
Zoolander
Zoolander 2
Zoom
Zulu
"""
    keys = ['Zero Dark Thirthy', 'Zulu',  'Zoomlander 2']
    comparisons = ['>=', '<=', '<>']
    logic = ['AND', 'AND', 'AND']
    print(movie_title_index.op(keys[0], comparisons[0]))
    print(movie_title_index.op(keys[1], comparisons[1]))
    print(movie_title_index.op(keys[2], comparisons[2]))
    # print(movie_title_index.multi_op(keys, comparisons, logic))

    print(movie_title_index.op('%Zooland%', 'LIKE'))

    print("SUCCESS")



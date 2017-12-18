from server.indexing.BitmapIndex import BitmapIndex

pairs = [(0,0,0),(1,10,1),(2,20,2),(3,30,3), (4,40,4), (5,50,5),(1,60,6)]

string_pairs = [("Batman", 529395, 4),
                ("Catwoman", 5920853, 5),
                ("Inglorious Bastards", 58297692, 0),
                ("Longest Yard", 59275394, 3),
                ("spiderman", 3903924, 1),
                ("spiderman 2", 3903925, 6),
                ("superman", 5829485, 2)
                ]

index = BitmapIndex.make(n for n in string_pairs)

# print("KEY 2 BITSTRING TREE")
# print(index.bitstringTree)
#
# print("RECORD 2 MEMORY TREE")
# print(index.recordsTree)

# print(index.bitstringTree[2].populate_record_list())
#
# n = index.recordsTree.n
# print("N")
# print(n)
#
# print(index.bitstringTree[2].decode_compressed_string(n))

print(index.like("%man 2%"))
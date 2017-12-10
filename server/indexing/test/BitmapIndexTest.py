from server.indexing.BitmapIndex import BitmapIndex

pairs = [(1,1,10),(2,2,20),(3,3,30), (4,4,40), (5,5,50),(1,6,12)]

index = BitmapIndex.make(n for n in pairs)

# print("KEY 2 BITSTRING TREE")
# print(index.bitstringTree)
#
# print("RECORD 2 MEMORY TREE")
# print(index.recordsTree)

print(index.bitstringTree[1].populate_record_list())
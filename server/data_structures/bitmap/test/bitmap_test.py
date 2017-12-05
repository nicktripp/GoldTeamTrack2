from server.data_structures.bitmap.BitmapIndex import BitmapIndex
from server.data_structures.bitmap.bitmap import Bitmap
from server.data_structures.btree.btree import BTree

btree = BTree(4, {4:[4,5,6,8, 13, 17], 6:[6, 10, 11, 13], 9:[9]})
btree[10] = [10,11]
btree[8] = [8]

bitmap = Bitmap("numbers", btree)

#print(bitmap)


index = BitmapIndex(btree)

print(index.string(4, 17))

print(index.do_or([4,6], 17))

print(index.do_and([4,6], 17))

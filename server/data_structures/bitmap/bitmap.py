import math

class Bitmap:

    # Each Bitmap will be made for a specific attribute
    # Pass in a tree and the key
    # Number of total records ?

    column = ""
    btree = None
    maps = []

    __init__(self, column_name, btree):
        self.column = column_name
        self.btree = btree

        # TODO: GO THROUGH B-TREE AND FIND NUMBER OF UNIQUE values

        items = btree.items

        for item in items:
            if item.key in maps:
                maps[item.key].append(item.record_number)
            else:
                maps.append(item.key:Bitmap_Entry(item.key, item.record_number))


        # TODO: COMPRESS BITSTRING

        for bitmap_entry in maps:
            bitstring = ""
            for x in bitmap_entry.record_list:
                num = record_list[x]
                i = math.ceil(math.log2(num))
                bitstring += ("1"* (i-1) + "0")
                bitstring += "{0:b}".format(num)

            bitmap_entry.compressed_bitstring = bitstring

import math

from server.data_structures.bitmap.Bitmap_Entry import Bitmap_Entry


class Bitmap:
    # Each Bitmap will be made for a specific attribute
    # Pass in a tree and the key
    # Number of total records ?


    def __init__(self, column_name, btree):
        self.column = column_name
        self.btree = btree
        self.maps = dict()

        # TODO: GO THROUGH B-TREE AND FIND NUMBER OF UNIQUE values

        #print(btree.items())

        for k, v in btree.items():
            if k in self.maps:
                #print(k)
                for each_v in v:
                    #print(each_v)
                    self.maps[k].append(each_v)
            else:
                #print("k:" + str(k))
                new_entry = Bitmap_Entry(k, v[0])
                for each_v in v[1:]:
                    #print("v:" + str(each_v))
                    new_entry.append(each_v)
                self.maps[k] = new_entry
            #print(self.maps)

        #print("maps")
        #print(self.maps)

        # TODO: COMPRESS BITSTRING



        for key, bitmap_entry in self.maps.items():
            bitstring = ""

            for num in range(len(bitmap_entry.record_list)):
                if num == 0:
                    #print("FIRST")
                    difference = bitmap_entry.record_list[num]
                else:
                    difference = bitmap_entry.record_list[num] - bitmap_entry.record_list[num-1] - 1

                i = math.ceil(math.log2(difference+1))
                bitstring += ("1" * (i - 1) + "0")
                bitstring += "{0:b}".format(difference)
                print(bitstring)

            bitmap_entry.compressed_bitstring = bitstring

    def __repr__(self):
        for key, bitmap in self.maps.items():
            print(str(key)+ ": " + bitmap.compressed_bitstring)
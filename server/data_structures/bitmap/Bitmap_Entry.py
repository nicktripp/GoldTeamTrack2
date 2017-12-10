import math


class Bitmap_Entry:


    def __init__(self, key, record_number):

        self.key = key

        self.record_list = []
        self.record_list.append(record_number)

        self.compressed_bitstring = ""

    def __repr__(self):
        return str(self.record_list)

    def append(self, record_number):
        self.record_list.append(record_number)

    def encode_compressed_bitstring(self):

        bitstring = ""
        for num in range(len(self.record_list)):
            if num == 0:
                # print("FIRST")
                difference = self.record_list[num]
            else:
                difference = self.record_list[num] - self.record_list[num - 1] - 1

            i = math.ceil(math.log2(difference + 1))
            bitstring += ("1" * (i - 1) + "0")
            bitstring += "{0:b}".format(difference)
            print(bitstring)

        self.compressed_bitstring = bitstring

    def decode_compressed_string(self):

        compressed = self.compressed_bitstring
        string = ""

        while (len(compressed) > 0):
            num_bits = compressed.find("0") + 1
            # print("Num Bits")
            # print(num_bits)
            compressed = compressed[num_bits:]
            # print("Compressed")
            # print(compressed)

            space = int(str(compressed[:num_bits]), 2)

            # print("Space")
            # print(space)
            string += "0" * space
            string += "1"
            # print("String")
            # print(string)

            compressed = compressed[num_bits:]
            # print("Compressed")
            # print(compressed)

    def populate_record_list(self):


        compressed = self.compressed_bitstring

        num_bits = compressed.find("0") + 1
        # print("Num Bits")
        # print(num_bits)
        compressed = compressed[num_bits:]
        # print("Compressed")
        # print(compressed)

        space = int(str(compressed[:num_bits]), 2)

        # print("Space")
        # print(space)

        if(len(self.record_list) == 0):
            self.record_list.append(space+1)
        else:
            self.record_list.append(self.record_list[-1]+space+1)
        # print("String")
        # print(string)

        compressed = compressed[num_bits:]
        # print("Compressed")
        # print(compressed)
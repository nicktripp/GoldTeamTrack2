import math


class Bitmap_Entry:


    def __init__(self, key, record_number):

        self.key = key
        self.record_list = []
        self.record_list.append(record_number)
        self.compressed_bitstring = ""
        self._int = 0

    def __repr__(self):
        return str(self.record_list)

    def append(self, record_number):
        self.record_list.append(record_number)

    def encode_compressed_bitstring(self):

        bitstring = ""
        for num in range(len(self.record_list)):
            if num == 0:
                difference = self.record_list[num]
            else:
                difference = self.record_list[num] - self.record_list[num - 1] - 1

            i = math.ceil(math.log2(difference + 1))
            bitstring += ("1" * (i - 1) + "0")
            bitstring += "{0:b}".format(difference)

        self.compressed_bitstring = bitstring
        self._int = int(bitstring,2)

    def decode_compressed_string(self, n):

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

        if (len(string) < n):
            string += "0" * (n - len(string))

        return string

    def populate_record_list(self):

        compressed = self.compressed_bitstring
        print("compressed")
        print(compressed)

        new_record_list = []

        while (len(compressed) > 0):
            num_bits = compressed.find("0") + 1
            compressed = compressed[num_bits:]

            space = int(str(compressed[:num_bits]), 2)

            print("space: " + str(space))
            # if(len(self.record_list) == 0):
            #     self.record_list.append(space+1)
            # else:
            #     self.record_list.append(self.record_list[-1]+space+1)


            if (len(new_record_list) == 0):
                new_record_list.append(space)
            else:
                new_record_list.append(new_record_list[-1]+space+1)

            compressed = compressed[num_bits:]

        return new_record_list

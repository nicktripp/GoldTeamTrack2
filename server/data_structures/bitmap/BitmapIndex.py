from server.data_structures.bitmap.bitmap import Bitmap


class BitmapIndex:

    def __init__(self, btree):

        self.bitmap = Bitmap("name", btree)

    def string(self, column_name, num_records):


        compressed = self.bitmap.maps[column_name].compressed_bitstring
        #print("Compressed")
        #print(compressed)
        string = ""


        while(len(compressed) > 0):

            num_bits = compressed.find("0") + 1
            #print("Num Bits")
            #print(num_bits)
            compressed = compressed[num_bits:]
            #print("Compressed")
            #print(compressed)

            space = int(str(compressed[:num_bits]), 2)

            #print("Space")
            #print(space)
            string += "0" * space
            string += "1"
            #print("String")
            #print(string)

            compressed = compressed[num_bits:]
            #print("Compressed")
            #print(compressed)
        leng = len(string)
        if(leng < num_records):
            endingZeros = "0"*((num_records - leng)+1)
            string += endingZeros

        return string


    def __repr__(self):
        return None


    def do_or(self, column_names, num_records):

        the_string = "0" * (num_records+1)
        for column in column_names:

            print(("Column"))
            print(column)

            new_string = self.string(column, num_records)
            print("new string")
            print(new_string)
            print("the_string")
            print(the_string)

            zipstring = zip(the_string, new_string)
            temp_string = ""
            for a,b in zipstring:
                temp_string += str(int(a,2) | int(b,2))
                print(temp_string)

            the_string = temp_string

        return the_string

    def do_and(self, column_names, num_records):

        the_string = "1" * (num_records+1)
        for column in column_names:

            print(("Column"))
            print(column)

            new_string = self.string(column, num_records)
            print("new string")
            print(new_string)
            print("the_string")
            print(the_string)

            zipstring = zip(the_string, new_string)
            temp_string = ""
            for a,b in zipstring:
                temp_string += str(int(a,2) & int(b,2))
                print(temp_string)

            the_string = temp_string

        return the_string
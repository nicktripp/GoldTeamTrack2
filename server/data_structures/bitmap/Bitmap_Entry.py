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

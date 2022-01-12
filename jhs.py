import io
import os
import random
# from torch import IterableDataset
from collections import OrderedDict


class JHS:
    def __init__(self, file_name: str, map_name: str, mode: str):
        if not file_name.endswith('.jhs'):
            raise Exception('file extension must be .jhs')
        if not map_name.endswith('.jhsmap'):
            raise Exception('map extension must be .jhsmap')
        if mode not in ['w', 'r', 'rc', 'a']:
            raise Exception('invalid file mode')

        self.file_name = file_name
        self.map_name = map_name
        self.mode = mode

        self.total_bytes = 0
        self.__file_stream = None
        self.__map_stream = None
        self.__copy_file_stream = None
        self.__copy_map_stream = None
        self.__identifiers = None
        self.__copy_row_order = OrderedDict()

        # setup tasks
        if self.mode == 'w':
            self.__open_write()
        elif self.mode == 'r':
            self.__open_read()
        elif self.mode == 'rc':
            self.__open_read()
            self.__init_copy()
        elif self.mode == 'a':
            self.__open_append()

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_value, ex_traceback):
        self.__close()

    def __open_append(self):
        self.__file_stream = open(self.file_name, 'ab')
        self.__map_stream = open(self.map_name, 'a+')

        self.__map_stream.seek(0)
        line = self.__map_stream.readline()
        while line:
            num_bytes = int(line.split()[-1])
            self.total_bytes += num_bytes
            line = self.__map_stream.readline()
        self.__map_stream.seek(0, io.SEEK_END)

    def __open_write(self):
        # create file
        self.__file_stream = open(self.file_name, 'wb')
        self.__map_stream = open(self.map_name, 'w')

    def __open_read(self):
        self.__file_stream = open(self.file_name, 'rb')
        self.__map_stream = open(self.map_name, 'r')

    def __init_copy(self):
        self.__copy_file_name = self.file_name + '.tmp_copy'
        self.__copy_map_name = self.map_name + '.tmp_copy'
        self.__copy_file_stream = open(self.__copy_file_name, 'wb')
        self.__copy_map_stream = open(self.__copy_map_name, 'w')

        size_needed = os.path.getsize(self.file_name)
        self.__copy_file_stream.truncate(size_needed)  # allocate space for full file

        items = []
        self.__map_stream.seek(0)
        line = self.__map_stream.readline()
        while line:
            identifier, _, block_size = line.split()
            items.append((identifier, int(block_size)))
            line = self.__map_stream.readline()
        self.__map_stream.seek(0)
        random.shuffle(items)

        next_begin_byte = 0
        self.__copy_map_stream.seek(0)
        for item in items:
            identifier = item[0]
            block_size = item[1]
            self.__copy_row_order[identifier] = (next_begin_byte, block_size)
            self.__copy_map_stream.write('{}\t{}\t{}\n'.format(identifier, next_begin_byte, block_size))
            next_begin_byte = next_begin_byte + block_size

    def __get_begin_byte_for_copy(self, identifier: str):
        return self.__copy_row_order[identifier][0]

    def __close(self):
        self.__file_stream.close()
        self.__map_stream.close()

        if self.mode == 'rc':
            self.__copy_file_stream.close()
            self.__copy_map_stream.close()

            # remove old
            os.remove(self.file_name)
            os.remove(self.map_name)

            # rename new
            os.rename(self.__copy_file_name, self.file_name)
            os.rename(self.__copy_map_name, self.map_name)

    # close = __close

    def write_next_block(self, data: bytes, identifier: str):
        assert type(data) is bytes
        block_size = len(data)  # number of bytes in bytes-string
        begin_byte = self.total_bytes  # should this be +1 ??
        self.__map_stream.write('{}\t{}\t{}\n'.format(identifier, begin_byte, block_size))
        self.__file_stream.write(data)
        self.total_bytes += block_size

    def read_next_block(self):
        line = self.__map_stream.readline()
        if not line:
            return False, False
        identifier, begin_byte, block_size = line.split()
        begin_byte, block_size = int(begin_byte), int(block_size)
        data = self.__file_stream.read(block_size)

        if self.mode == 'rc':
            copy_begin_byte = self.__get_begin_byte_for_copy(identifier)
            self.__copy_file_stream.seek(copy_begin_byte)
            self.__copy_file_stream.write(data)

        return data, identifier

# class TripletDataset(JHS, IterableDataset):
# 	def __init__(self, file_name, map_name):
# 		JHS.__init__(self, file_name, map_name, 'rc')
# 		# now deal with iterable dataset things...?

# 	def write_triplet(self, pair_name, plm, cov, pre):
# 		buff = io.BytesIO()
# 		torch.save(plm, buff)
# 		buff.seek(0)
# 		JHS.write_next_block(self, data=buff.read(), identifier=pair_name+'-PLM')

# 		buff = io.BytesIO()
# 		torch.save(cov, buff)
# 		buff.seek(0)
# 		JHS.write_next_block(self, data=buff.read(), identifier=pair_name+'-COV')

# 		buff = io.BytesIO()
# 		torch.save(pre, buff)
# 		buff.seek(0)
# 		JHS.write_next_block(self, data=buff.read(), identifier=pair_name+'-PRE')


# 	def read_next_triplet(self, map_location='cpu'):
# 		plm_bytes = super().read_next_block()
# 		cov_bytes = super().read_next_block()
# 		pre_bytes = super().read_next_block()

# 		plm = torch.load(f=io.BytesIO(plm_bytes), map_location=map_location)
# 		cov = torch.load(f=io.BytesIO(cov_bytes), map_location=map_location)
# 		pre = torch.load(f=io.BytesIO(pre_bytes), map_location=map_location)

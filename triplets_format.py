import io
import os
import random
from collections import OrderedDict
import numpy as np


class TripletsFormat:
    def __init__(self, file_name: str, map_name: str, mode: str):
        if not file_name.endswith('.bigdata'):
            raise Exception('data file extension must be .bigdata')
        if not map_name.endswith('.bigmap'):
            raise Exception('map file extension must be .bigmap')
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
        self.close()

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

    def __get_num_bytes(self, identifier: str):
        return self.__copy_row_order[identifier][1]

    @staticmethod
    def __num_bytes_to_L(num):
        return int(np.sqrt(num / 441 / 2))

    def close(self):
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

    @staticmethod
    def __encode(data):
        # formatted to binary
        assert type(data) == np.ndarray
        assert data.dtype == 'float16'
        assert data.shape[0] == 441
        assert data.shape[1] == data.shape[2]  # 441 x L x L
        L = data.shape[1]
        data_bytes = data.tobytes(order='C')
        assert TripletsFormat.__num_bytes_to_L(len(data_bytes)) == L
        return data_bytes

    @staticmethod
    def __decode(b):
        num_bytes = len(b)
        L = TripletsFormat.__num_bytes_to_L(num_bytes)
        data = np.frombuffer(b, dtype='float16').reshape(441, L, L)
        return data

    def write_next_np(self, data_np: np.ndarray, identifier: str):
        data_bytes = TripletsFormat.__encode(data_np)
        block_size = len(data_bytes)  # number of bytes in bytes-string
        begin_byte = self.total_bytes
        self.__map_stream.write('{}\t{}\t{}\n'.format(identifier, begin_byte, block_size))
        self.__file_stream.write(data_bytes)
        self.total_bytes += block_size

    def read_next_np(self):
        line = self.__map_stream.readline()
        if not line:
            return None, None
        identifier, begin_byte, block_size = line.split()
        begin_byte, block_size = int(begin_byte), int(block_size)
        data_bytes = self.__file_stream.read(block_size)
        data_np = TripletsFormat.__decode(data_bytes)

        if self.mode == 'rc':
            copy_begin_byte = self.__get_begin_byte_for_copy(identifier)
            self.__copy_file_stream.seek(copy_begin_byte)
            self.__copy_file_stream.write(data_bytes)

        return data_np, identifier

    def __iter__(self):
        return self

    def __next__(self):
        return self.read_next_np()
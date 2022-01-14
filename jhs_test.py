import io
import msgpack
import msgpack_numpy as m
from triplets_format import TripletsFormat
import numpy as np

m.patch()

arr1 = np.random.rand(441, 1, 1).astype('float16')
arr2 = np.random.rand(441, 1000, 1000).astype('float16')
arr3 = np.random.rand(441, 5, 5).astype('float16')
arr4 = np.random.rand(441, 50, 50).astype('float16')


with TripletsFormat(file_name='temp.bigdata', map_name='map.bigmap', mode='w') as j:
	j.write_next_np(arr1, '#1')
	j.write_next_np(arr2, '#2')
	j.write_next_np(arr3, '#3')



with TripletsFormat(file_name='temp.bigdata', map_name='map.bigmap', mode='a') as j:
	j.write_next_np(arr4, '#4')


with TripletsFormat(file_name='temp.bigdata', map_name='map.bigmap', mode='r') as j:
	result, identifier = j.read_next_np()
	assert np.all(result == arr1)

	result, identifier = j.read_next_np()
	assert np.all(result == arr2)

	result, identifier = j.read_next_np()
	assert np.all(result == arr3)

	result, identifier = j.read_next_np()
	assert np.all(result == arr4)


with TripletsFormat(file_name='temp.bigdata', map_name='map.bigmap', mode='rc') as j:
	result, identifier = j.read_next_np()
	assert np.all(result == arr1)

	result, identifier = j.read_next_np()
	assert np.all(result == arr2)

	result, identifier = j.read_next_np()
	assert np.all(result == arr3)

	result, identifier = j.read_next_np()
	assert np.all(result == arr4)


d = {
	'#1': 'arr1',
	'#2': 'arr2',
	'#3': 'arr3',
	'#4': 'arr4'
}


with TripletsFormat(file_name='temp.bigdata', map_name='map.bigmap', mode='r') as j:
	result, identifier = j.read_next_np()
	while identifier:
		arr = locals()[d[identifier]]
		assert np.all(result == arr)
		result, identifier = j.read_next_np()

print('ALL PASS')
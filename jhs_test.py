import io
import msgpack
import msgpack_numpy as m
from jhs import JHS
import numpy as np

m.patch()

block1 = b'12345678'
block2 = b'qwertyuiop'
arr = np.random.rand(100,100)
block3 = msgpack.packb(arr)
block4 = b'my name is jake and it is not eric'




with JHS(file_name='temp.jhs', map_name='map.jhsmap', mode='w') as j:
	j.write_next_block(data=block1,identifier='#1')
	j.write_next_block(data=block2,identifier='#2')
	j.write_next_block(data=block3,identifier='#3')



with JHS(file_name='temp.jhs', map_name='map.jhsmap', mode='a') as j:
	j.write_next_block(data=block4,identifier='#4')


with JHS(file_name='temp.jhs', map_name='map.jhsmap', mode='r') as j:
	result, identifier = j.read_next_block()
	assert result == block1

	result, identifier = j.read_next_block()
	assert result == block2

	result, identifier = j.read_next_block()
	assert result == block3

	a = msgpack.unpackb(block3)
	assert np.allclose(arr,a)

	result, identifier = j.read_next_block()
	assert result == block4


with JHS(file_name='temp.jhs', map_name='map.jhsmap', mode='rc') as j:
	result, identifier = j.read_next_block()
	assert result == block1

	result, identifier = j.read_next_block()
	assert result == block2

	result, identifier = j.read_next_block()
	assert result == block3

	a = msgpack.unpackb(block3)
	assert np.allclose(arr, a)

	result, identifier = j.read_next_block()
	assert result == block4


d = {
	'#1': 'block1',
	'#2': 'block2',
	'#3': 'block3',
	'#4': 'block4'
}


with JHS(file_name='temp.jhs', map_name='map.jhsmap', mode='r') as j:
	result, identifier = j.read_next_block()
	while result:
		block = locals()[d[identifier]]
		if identifier != '#3':
			assert result == block
		else:
			a = msgpack.unpackb(block)
			assert np.allclose(arr, a)
		result, identifier = j.read_next_block()

print('ALL PASS')
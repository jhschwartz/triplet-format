from jhs import JHS
import torch, io
block1 = b'12345678'
block2 = b'qwertyuiop'
tensor = torch.rand((100,100),dtype=torch.half)
buff = io.BytesIO()
torch.save(tensor,buff)
buff.seek(0)
block3 = buff.read()
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

	t = torch.load(io.BytesIO(block3))
	assert torch.allclose(tensor,t)

	result, identifier = j.read_next_block()
	assert result == block4


with JHS(file_name='temp.jhs', map_name='map.jhsmap', mode='rc') as j:
	result, identifier = j.read_next_block()
	assert result == block1

	result, identifier = j.read_next_block()
	assert result == block2

	result, identifier = j.read_next_block()
	assert result == block3

	t = torch.load(io.BytesIO(block3))
	assert torch.allclose(tensor,t)

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
			t = torch.load(io.BytesIO(block))
			assert torch.allclose(tensor,t)
		result, identifier = j.read_next_block()

print('ALL PASS')
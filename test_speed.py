#!/home/jaschwa/.miniconda3/envs/SLIPPI-ftr/bin/python

import sys, os
sys.path.append(os.getcwd())

from triplets_format import TripletsFormat
import numpy as np
import torch
import time

d = '/scratch/sigbio_project_root/sigbio_project1/jaschwa/SLIPPI/features/out/prot1-prot2/'
plm = torch.load(d+'prot1-prot2.plm.fea',map_location=torch.device('cpu')).numpy()
cov = torch.load(d+'prot1-prot2.cov.fea',map_location=torch.device('cpu')).numpy()
pre = torch.load(d+'prot1-prot2.pre.fea',map_location=torch.device('cpu')).numpy()


dataf = '/scratch/sigbio_project_root/sigbio_project1/jaschwa/triplets.bigdata'
mapf = '/scratch/sigbio_project_root/sigbio_project1/jaschwa/tripletsmap.bigmap'

with TripletsFormat(dataf, mapf, 'w') as f:
	t = time.time()
	for i in range(5):
		print(i)
		f.write_next_np(plm, str(i)+'-PLM')
		f.write_next_np(cov, str(i)+'-COV')
		f.write_next_np(pre, str(i)+'-PRE')
	e = time.time()-t
	mins = e % 60
	secs = e - mins*60
	print('write 1000 triplets took {} minutes {} seconds'.format(mins, secs))

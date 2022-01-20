#!/home/jaschwa/.miniconda3/envs/SLIPPI-ftr/bin/python

import sys, os
sys.path.append(os.getcwd())

from triplets_format import TripletsFormat

try:
    if not len(sys.argv) == 2:
        print('error: not enough arguments')
        raise Exception
    if not sys.argv[1].endswith('.aln'):
        print('error: msa must end with .aln')
        raise Exception
except Exception:
    print('USAGE: ./computeFeatureTriplet.py msa.aln ppiName outfile-cov outfile-pre outfile-plm')
    print('Feature triplet generation for SLIPPI')
    exit(-1)


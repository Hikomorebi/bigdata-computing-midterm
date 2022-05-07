# !/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'komorebi'

# import pickle
#
# with open('tmp/in_table6','rb') as f:
#      dict = pickle.load(f)
# print(dict)
#
# with open('tmp/out_table3','rb') as f:
#      dict = pickle.load(f)
# print(dict)

d = {'a': 1, 'b': 4, 'c': 2, 'f': 12}
a1 = sorted(d.items(), key=lambda x: x[1], reverse=True)
with open('./result.txt','w') as f:
    for _ in range(3):
        str1 = str(a1[_][0]) + ' ' + str(a1[_][1]) + '\n'
        f.write(str1)



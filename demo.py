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
a = {'ab':3,"dd":6,'ee':1}
print(sorted(a.items(), key = lambda kv:(kv[1], kv[0])))

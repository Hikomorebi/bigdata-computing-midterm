# !/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'komorebi'

import pickle
import os


tmp_dict = {}
iter_dict = {}
out_count = {}
no_out_node = set() #出度为0的节点集合，即deadend
no_in_node = set()

no_in_node_list = []
node_count = 0
teleport_par = 0.85

for _ in range(10):
    no_in_node_list.append(set())

if os.path.exists('./tmp/node_set'):
    with open('./tmp/node_set', 'rb') as f1:
        node_set = pickle.load(f1)
else:
    print('未找到/tmp/node_set文件，请先运行bs.py')


with open('./tmp/no_out_node','rb') as f:
    no_out_node = pickle.load(f)
with open('./tmp/no_in_node','rb') as f:
    no_in_node = pickle.load(f)
node_count = len(node_set)

for _ in no_in_node:
    no_in_node_list[_ // 1000].add(_)
'''
接下来获取每一个节点的出度
'''
for _ in range(10):
    if os.path.exists('./tmp/out_table' + str(_)):
        with open('./tmp/out_table' + str(_), 'rb') as f:
            tmp_dict = pickle.load(f)
            for key in tmp_dict.keys():
                out_count[key] = len(tmp_dict[key])
            tmp_dict.clear()

# for _ in node_set:    #输出每个节点对应的出度
#     print(str(_)+':'+str(out_count[_]))
#print(len(no_out_node))


'''
计算迭代矩阵，并将该矩阵分块存储
'''
for _ in range(10):
    if os.path.exists('./tmp/in_table' + str(_)):
        with open('./tmp/in_table' + str(_), 'rb') as f:
            tmp_dict = pickle.load(f)
        for to_node in tmp_dict.keys():
            iter_dict[to_node] = {}
            for from_node in node_set:
                if from_node in tmp_dict[to_node]:
                    iter_dict[to_node][from_node] = teleport_par * 1 / out_count[from_node] + (1 - teleport_par) * 1 / node_count
                elif from_node in no_out_node:
                    iter_dict[to_node][from_node] = 1 / node_count
                else:
                    iter_dict[to_node][from_node] = (1 - teleport_par) * 1 / node_count
        for to_node in no_in_node_list[_]:  #处理入度为0的节点
            iter_dict[to_node] = {}
            for from_node in node_set:
                if from_node in no_out_node:
                    iter_dict[to_node][from_node] = 1 / node_count
                else:
                    iter_dict[to_node][from_node] = (1 - teleport_par) * 1 / node_count
        with open('./tmp/iter_table'+str(_),'wb+') as f:
            pickle.dump(iter_dict,f)
        iter_dict.clear()



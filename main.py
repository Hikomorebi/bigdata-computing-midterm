# !/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'komorebi'

import pickle
import os

tmp_dict = {}
out_count = {}
node_count = 0
teleport_par = 0.85 #设置teleport parameter
stop_loss = 0.0000001
loss = 1
old_rank = {}
new_rank = {}


if os.path.exists('./tmp/node_set'):
    with open('./tmp/node_set', 'rb') as f1:
        node_set = pickle.load(f1)
else:
    print('未找到/tmp/node_set文件，请先运行bs.py')

node_count = len(node_set)

for _ in node_set:
    old_rank[_] = 1 / node_count
    new_rank[_] = 0

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


while loss > stop_loss:
    for _ in range(10):
        if os.path.exists('./tmp/in_table' + str(_)):
            with open('./tmp/in_table' + str(_), 'rb') as f:
                tmp_dict = pickle.load(f)
            for to_node in tmp_dict.keys():
                sum = 0
                for from_node in tmp_dict[to_node]:
                    sum += teleport_par * (old_rank[from_node] / out_count[from_node])
                new_rank[to_node] = sum
            tmp_dict.clear()
    acc = 0
    for _ in node_set:
        acc += new_rank[_]
    temp = (1 - acc) / node_count
    for _ in node_set:
        new_rank[_] += temp
    loss = 0
    for _ in node_set:
        loss += abs(old_rank[_] - new_rank[_])
        old_rank[_] = new_rank[_]
        new_rank[_] = 0

sort_list = sorted(old_rank.items(), key=lambda x: x[1], reverse=True)  #对结果进行排序

with open('./result.txt','w') as f: #将top100节点信息输出至result.txt
    for _ in range(100):
        str1 = str(sort_list[_][0]) + '    ' + str(sort_list[_][1]) + '\n'
        f.write(str1)


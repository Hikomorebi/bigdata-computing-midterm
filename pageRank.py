# !/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'komorebi'

import pickle
import os

stop_loss = 0.001
loss = 999
node_set = set()
no_in_node = set()  #没有入度的节点
iter_dict = {}
if not os.path.exists('./tmp'):
    print('未找到/tmp目录，请先运行bs.py和bs2.py')
if os.path.exists('./tmp/node_set'):
    with open('./tmp/node_set', 'rb') as f:
        node_set = pickle.load(f)
if os.path.exists('./tmp/no_in_node'):
    with open('./tmp/no_in_node', 'rb') as f:
        no_in_node = pickle.load(f)

old_rank = {}
new_rank = {}
node_count = len(node_set)
for _ in node_set:
    old_rank[_] = 1 / node_count
    new_rank[_] = 0


while loss > stop_loss:
    for _ in range(10):
        if os.path.exists('./tmp/iter_table' + str(_)):
            with open('./tmp/iter_table' + str(_), 'rb') as f:
                iter_dict = pickle.load(f)
                for to_node in iter_dict.keys():
                    sum = 0
                    for from_node in iter_dict[to_node].keys():
                        sum += iter_dict[to_node][from_node] * old_rank[from_node]
                    new_rank[to_node] = sum
    loss = 0
    for _ in node_set:
        loss += abs(old_rank[_] - new_rank[_])
        old_rank[_] = new_rank[_]

# for _ in range(10):
#     if os.path.exists('./tmp/iter_table' + str(_)):
#         with open('./tmp/iter_table' + str(_), 'rb') as f:
#             iter_dict = pickle.load(f)
#             for to_node in iter_dict.keys():
#                 sum = 0
#                 for from_node in iter_dict[to_node].keys():
#                     sum += iter_dict[to_node][from_node] * old_rank[from_node]
#                 new_rank[to_node] = sum
# loss = 0
# for _ in node_set:
#     loss += abs(old_rank[_] - new_rank[_])
#     old_rank[_] = new_rank[_]
#print(new_rank)

print(sorted(new_rank.items(), key = lambda kv:(kv[1], kv[0])))





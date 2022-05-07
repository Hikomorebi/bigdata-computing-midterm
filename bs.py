# !/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'komorebi'

import pickle
import os


in_table_list = []
in_table_count = []
out_table_list = []
out_table_count = []
node_set = set()    #以集合的形式记录所有id
tmp_dict = {}
#max_id = 0
max_count = 500 #每个块能在内存中存储多少个元素
from_index = 0
to_index = 0
try:
    os.mkdir('tmp')
except FileExistsError:
    print('tmp目录已存在！')
    exit(0)
for i in range(10): #分为10个块
    in_table_list.append({})
    in_table_count.append(0)
    out_table_list.append({})
    out_table_count.append(0)


with open('data.txt','r') as f:
    while True:
        line = f.readline().strip()
        if not line:    #读到文件末尾，结束文件读取
            break
        str_list = line.split(' ')
        assert(len(str_list) == 2)  #from_node to_node
        from_node = eval(str_list[0])
        to_node = eval(str_list[1])
        node_set.add(from_node)
        node_set.add(to_node)
        from_node_set.add(from_node)
        to_node_set.add(to_node)
        from_index = from_node // 1000
        to_index = to_node // 1000      #获取应当所在块
        if to_node in in_table_list[to_index]:
            in_table_list[to_index][to_node].append(from_node)
        else:
            in_table_list[to_index][to_node] = [from_node]
        in_table_count[to_index] += 1   #该块在内存中的存储元素+1
        if from_node in out_table_list[from_index]:
            out_table_list[from_index][from_node].append(to_node)
        else:
            out_table_list[from_index][from_node] = [to_node]
        out_table_count[from_index] += 1

        if in_table_count[to_index] >= max_count:   #in_index对应的in_table字典中存储元素超过max_count的值
            if os.path.exists('./tmp/in_table' + str(to_index)): #判断是否为第一次存入磁盘
                with open('./tmp/in_table' + str(to_index),'rb') as f1:
                    tmp_dict = pickle.load(f1)
                    for key in in_table_list[to_index].keys():
                        if key in tmp_dict:
                            tmp_dict[key].extend(in_table_list[to_index][key])
                        else:
                            tmp_dict[key] = in_table_list[to_index][key]
                with open('./tmp/in_table' + str(to_index), 'wb+') as f1:
                    pickle.dump(tmp_dict, f1)
                tmp_dict.clear()
            else:
                with open('./tmp/in_table' + str(to_index), 'wb+') as f1:
                    pickle.dump(in_table_list[to_index], f1)
            in_table_list[to_index].clear() #清楚该块占用的内存
            in_table_count[to_index] = 0    #重新计数

        if out_table_count[from_index] >= max_count:  # out_index对应的out_table字典中存储元素超过max_count的值
            if os.path.exists('./tmp/out_table' + str(from_index)):  # 判断是否为第一次存入磁盘，如果不是，则需要将该块从磁盘中取出，合并后再存入磁盘
                with open('./tmp/out_table' + str(from_index), 'rb') as f1:
                    tmp_dict = pickle.load(f1)
                    for key in out_table_list[from_index].keys():   #合并字典
                        if key in tmp_dict:
                            tmp_dict[key].extend(out_table_list[from_index][key])
                        else:
                            tmp_dict[key] = out_table_list[from_index][key]
                with open('./tmp/out_table' + str(from_index), 'wb+') as f1:
                    pickle.dump(tmp_dict, f1)
                tmp_dict.clear()
            else:
                with open('./tmp/out_table' + str(from_index), 'wb+') as f1:
                    pickle.dump(out_table_list[from_index], f1)
            out_table_list[from_index].clear() #清楚该块占用的内存
            out_table_count[from_index] = 0 #重新计数

#最后将内存中所有块中元素存入磁盘
for _ in range(10):
    if in_table_count[_] != 0:
        if os.path.exists('./tmp/in_table' + str(_)):  # 判断是否为第一次存入磁盘
            with open('./tmp/in_table' + str(_), 'rb') as f1:
                tmp_dict = pickle.load(f1)
                for key in in_table_list[_].keys():
                    if key in tmp_dict:
                        tmp_dict[key].extend(in_table_list[_][key])
                    else:
                        tmp_dict[key] = in_table_list[_][key]
            with open('./tmp/in_table' + str(_), 'wb+') as f1:
                pickle.dump(tmp_dict, f1)
            tmp_dict.clear()
        else:
            with open('./tmp/in_table' + str(_), 'wb+') as f1:
                pickle.dump(in_table_list[_], f1)
        in_table_list[_].clear()  # 清楚该块占用的内存
    if out_table_count[_] != 0:
        if os.path.exists('./tmp/out_table' + str(_)):  # 判断是否为第一次存入磁盘，如果不是，则需要将该块从磁盘中取出，合并后再存入磁盘
            with open('./tmp/out_table' + str(_), 'rb') as f1:
                tmp_dict = pickle.load(f1)
                for key in out_table_list[_].keys():  # 合并字典
                    if key in tmp_dict:
                        tmp_dict[key].extend(out_table_list[_][key])
                    else:
                        tmp_dict[key] = out_table_list[_][key]
            with open('./tmp/out_table' + str(_), 'wb+') as f1:
                pickle.dump(tmp_dict, f1)
            tmp_dict.clear()
        else:
            with open('./tmp/out_table' + str(_), 'wb+') as f1:
                pickle.dump(out_table_list[_], f1)
        out_table_list[_].clear()  # 清楚该块占用的内存

with open(('./tmp/node_set'), 'wb+') as f:  #以集合的形式存入所有节点的id
    pickle.dump(node_set, f)

# with open('temp.txt','wb') as f:
#     pickle.dump(in_table_list[0],f)
# with open('temp.txt','rb') as f:
#      dict = pickle.load(f)
#      print(dict)

from enum import Enum
import re

from utils.index import json, get_list_index_range


# data = {
#     "name": "harrison",
#     "age": 30,
#     "love": ["lanny", "bozi"]
# }
# result = json('/test.json', data)
# print(result)

# def get_dict_key_by_index(data: dict, index: int):
#     # 5   -1 -2 -3 -4 -5 -6
#     _list = list(data.keys())
#     print(_list)
#     if abs(index) >= len(_list):
#         return None
#     return _list[index]
#
#
# data = {'1_1': ['1_1', '2_1']}
#
# print(get_dict_key_by_index(data, -1))
# text = '1，49a，012,802.23he哈哈'
# pattern = "\d"
# print(re.search(pattern, text))

def large_num_format(large_num: str):
    regular = r'\d{1,3},(\d{3},)*(\d{3})(.\d+)?|[1-9]\d{1,2}'
    if large_num is None:
        return None
    elif re.findall(regular, large_num):
        large_num.replace('，', '').replace(',', '')
        return float(large_num)
    return large_num


# 如果是大额数字，处理为浮点数后返回
# 如果不是大额数字，原样返回
# demo = " 123,456,78.00   \n"
# print(demo.strip())
#
# import re
# threeNumRegex = re.compile(r'''
# (?:(?<![\d\,])\d{1,3}(?=\s)) #匹配只有1-3位数，左边
# |(?<![\d\,])(?:\d{1,3})(?:\,\d{3})+(?=\s) #匹配带有","的情况''', re.VERBOSE)
# /^\d{1,3},(\d{3},)*(\d{3})(.\d+)?$
pattern = r'(\d{1,3}),((\d{3},)*)(\d{3})(.\d+)?|[1-9]\d{1,2}'
large_num = "1,234,567,890,222,111.22"
match = re.findall(pattern, large_num)
print(match)
# print(match)
# print(match.groups())
# if match:
#     gs = match.groups()
#     result = gs[0] + gs[1] + gs[3] + gs[4]
#     print(result.replace(',', ''))
# result = large_num_format(large_num)
# print(result)

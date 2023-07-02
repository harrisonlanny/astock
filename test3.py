from collections.abc import Callable, Iterable, Mapping
import math
import time
import threading
from typing import Any
from service.report import STATIC_ANNOUNCEMENTS_DIR, parse_pdf

from utils.index import concurrency, get_path, txt,json,_filter

# pdf_name = "834599__同力股份__2022年年度报告__1216458687"
# pdf_url = get_path(f'{STATIC_ANNOUNCEMENTS_DIR}/{pdf_name}.pdf')
# parse_pdf(pdf_url, pdf_name=pdf_name)

scan_colors = json("/scan_color.json")
print("总数：",len(scan_colors))

# 1. 解决line_count==0，就解决了2/3的问题
# line_count_zero_list = _filter(scan_colors, lambda item: item['line']['count'] == 0)
# print("line count为0的个数：", len(line_count_zero_list))

# # 2. 剩下不等于0的里面，哪些line的个数不到rect的1/n
# n = 10
# line_count_not_zero_list = _filter(scan_colors, lambda item: item['line']['count'] != 0)
# line_count_very_small_list = _filter(line_count_not_zero_list, lambda item: item['rect']['count']/item['line']['count'] > n)
# print("line count极小的个数: ", len(line_count_very_small_list))

# line_count_import_list =  _filter(line_count_not_zero_list, lambda item: item['rect']['count']/item['line']['count'] <= n)
# print(len(line_count_import_list))

# json("/scan_color_line_important.json",line_count_import_list)

def get_color_list(color_dict: dict):
    arr: list[list] = []
    for color in color_dict:
        count = color_dict[color]
        arr.append([
            color,
            count
        ])
    arr.sort(reverse=True, key=lambda item: (item[1], item[0]))
    return arr

line_count_small_list = _filter(
    scan_colors, 
    lambda item: item['line']['count'] == 0 or item['rect']['count']/item['line']['count'] >= 10
)
print('line占比极小的个数：', len(line_count_small_list))

rect_has_2_color_list = []
for item in line_count_small_list:
    rect = item['rect']
    rect_color_dict = rect['color']
    rect_color_list = get_color_list(rect_color_dict)
    first_color = rect_color_list[0]
    if len(rect_color_list) >= 2:
        second_color = rect_color_list[1]
        if second_color[1]/first_color[1] >= 0.2:
            rect_has_2_color_list.append(item)
json("rect_2_color.json", rect_has_2_color_list)
print(len(rect_has_2_color_list))

# 834599__同力股份__2022年年度报告__1216458687
# 831304__迪尔化工__2022年年度报告__1216547848
# 


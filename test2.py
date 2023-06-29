# from db.index import read_table
# from service.report import STATIC_ANNOUNCEMENTS_DIR,STATIC_ANNOUNCEMENTS_PARSE_DIR, get_color_statistic, parse_pdf
# from utils.index import get_path
# import os

# urls = []
# r = read_table(table_name="announcements",
#                fields=["file_title"],
#                result_type="dict",
#                filter_str="where title not like '%英文%' and title not like '%取消%' and title not like '%摘要%' limit 2")

# for index, rs in enumerate(r):
#     file_title = rs['file_title']
#     pdf_url = get_path(f'{STATIC_ANNOUNCEMENTS_DIR}/{file_title}.pdf')
#     table_json_url = get_path(f'{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{file_title}__table.json')
#     print(pdf_url, f" {index + 1}/{len(r)}")
#     exists = os.path.exists(table_json_url)
#     if not exists:
#         parse_pdf(pdf_url, pdf_name=file_title)


# url = "./static/announcements/000001__平安银行__2017年年度报告__1204477157.pdf"
# get_color_statistic(url)
# pdf_url = get_path(f'{STATIC_ANNOUNCEMENTS_DIR}/000001__平安银行__2021年年度报告__1212533413.pdf')
# parse_pdf(pdf_url, pdf_name='000001__平安银行__2021年年度报告__1212533413.pdf')


from collections.abc import Callable, Iterable, Mapping
import math
import time
import threading
from typing import Any

from utils.index import txt

v = 10

arr = list(range(1,v+1))


def handle(start_index, end_index):
    for i,item in enumerate(arr[start_index:end_index+1]):
        index = start_index + i
        arr[index] = arr[index] + 0.1

# middle = math.ceil(len(arr) / 2)
# print(middle)

# 0,middle
# middle+1, len-1

# 不采用并发的耗时：
def old_function():
    start_time = time.time()

    handle(0, v)

    end_time = time.time()
    cost_time = f"{end_time-start_time:.2f}"
    print(f"不采用并发的耗时：{cost_time}")
    # print(arr)

# 采用并发的耗时

class MyThread(threading.Thread):
    def __init__(self,start_index, end_index):
        self.start_index = start_index
        self.end_index = end_index
        super().__init__()

    def run(self):
        handle(self.start_index, self.end_index)
    



def new_function():
    start_time = time.time()

    length = len(arr)
    middle = math.ceil(length / 2)
    t1 = MyThread(start_index=0, end_index=middle)
    t1.start()
    t2 = MyThread(start_index=middle+1, end_index=length-1)
    t2.start()

    end_time = time.time()
    cost_time = f"{end_time-start_time:.2f}"
    print(f"采用并发的耗时：{cost_time}")

old_function()

new_function()
txt("/test.txt", arr)
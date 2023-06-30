from collections.abc import Callable, Iterable, Mapping
import math
import time
import threading
from typing import Any

from utils.index import concurrency, txt

v = 50000000

arr = list(range(1,v+1))


def handle(start_index, end_index):
    for i,item in enumerate(arr[start_index:end_index+1]):
        index = start_index + i
        arr[index] = arr[index] + 0.1

# middle = math.ceil(len(arr) / 2)
# print(middle)

# 0,middle
# middle+1, len-1

# # 不采用并发的耗时：
# def old_function():
#     start_time = time.time()

#     handle(0, v)

#     end_time = time.time()
#     cost_time = f"{end_time-start_time:.2f}"
#     print(f"不采用并发的耗时：{cost_time}")
#     # print(arr)

# # 采用并发的耗时
# class MyThread(threading.Thread):
#     def __init__(self,start_index, end_index):
#         self.start_index = start_index
#         self.end_index = end_index
#         super().__init__()

#     def run(self):
#         handle(self.start_index, self.end_index)


# def new_function():
#     start_time = time.time()

#     length = len(arr)
#     middle = math.ceil(length / 2)
#     t1 = MyThread(start_index=0, end_index=middle)
#     t1.start()
#     t2 = MyThread(start_index=middle+1, end_index=length-1)
#     t2.start()

#     end_time = time.time()
#     cost_time = f"{end_time-start_time:.2f}"
#     print(f"采用并发的耗时：{cost_time}")

# old_function()

# new_function()
# txt("/test.txt", arr)






concurrency(
    None,
    list(range(0, 101)),
    count=3
)






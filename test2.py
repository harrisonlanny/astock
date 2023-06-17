# a = {"name": "harrison"}
# b = {"age": 12}
# # c = a.copy()
# # c.update(b)
# #
# # print(c)
#
# c = dict(a, **{
#     "age": 15
# })
# print(c)
from datetime import datetime,date

from utils.index import date2str

timestamp = 1681142400000
d = date.fromtimestamp(timestamp / 1000)
s = date2str(d, "%Y-%m-%d")
print(s)
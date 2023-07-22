
# from service.report import (
#     STATIC_ANNOUNCEMENTS_DIR,
#     STATIC_ANNOUNCEMENTS_PARSE_DIR,
#     get_color_statistic,
#     parse_pdf,
# )
# from utils.index import get_path, json


# namelist = [
#     "600862__中航高科__中航高科2022年年度报告__1216125992", # line特别少的，且只有一种主色
#     "834599__同力股份__2022年年度报告__1216458687", # line特别少的 且 主色有两种
#     "301129__瑞纳智能__2022年年度报告__1216435406", # line比rect多
#     "000534__万泽股份__2022年年度报告__1215991366",# rect比line多一点点
# ]

# # name = namelist[0]
# # 异常加粗线P42，P45，P186，P187，P200，P210
# # 未识别全边框P191

# # name = namelist[1]
# # 异常加粗线P54
# # 未识别全边框P141-P187（附注）

# name = namelist[2]
# # 异常加粗线P181
# # 未识别全边框P26，P28
# # 158,
# # name = namelist[3]
# # 未识别全边框P127-P217

# path = get_path(f"{STATIC_ANNOUNCEMENTS_DIR}/{name}.pdf")

# parse_pdf(path, name)


# from utils.index import get_path, is_exist

# _path = get_path("/db2")

# print(is_exist(_path))

# print("91_1".split("_")[1])
# import itertools
# from db.index import read_table
# from test import caculate_interest_bearing_liabilities_rate, calculate_interest_bearing_liabilities, get_total_assets
# from utils.index import _is_empty, _map, json



# c = "负债和所有者权益\n（或股东权益）总计"
# m = c.replace('\n', '').replace('（', '').replace('）', '') 
# print(m)

# from utils.index import _every


# cells = [None, None,2, None]
# print(_every(cells, lambda item: item is None))

# from utils.index import _is_empty


# text = set(list("哈哈哈"))
# chars = set(["。", "，", '“', "”", "："])

# print(_is_empty(text & chars))

# text = "反倒是佛菩萨福建师大批发哈合并资产负债表"
# key = "合并资产负债表"
# print(text[0:-len(key)])

from utils.index import is_chinese_number_prefix


text = "二十五、哈哈哈"
text1 = "（f一）xxx"
print(is_chinese_number_prefix(text1, False))
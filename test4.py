
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
from test import caculate_interest_bearing_liabilities_rate, calculate_interest_bearing_liabilities, get_total_assets
from utils.index import _is_empty, json


a = json("static/parse-announcements/hbzcfzb/003005__竞业达__2022年年度报告__1216617405__合并资产负债表.json")


i = calculate_interest_bearing_liabilities(a)
c=get_total_assets(a)
b = caculate_interest_bearing_liabilities_rate(i,c)

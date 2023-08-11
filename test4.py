
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

import re
from service.config import Financial_Statement
from db.index import read_table
from service.report import gen_hblrb, gen_hbzcfzb, generate_announcement, receivable_balance_propotion_of_monthly_average_operating_income
from strategy.announcements.announcements import filter_by_receivable_balance
from utils.index import _filter, _map, is_alabo_number_prefix, is_chinese_number_prefix, is_period_prefix, json


# text = "二十五、哈哈哈"
# text1 = "（f一）xxx"
# print(is_chinese_number_prefix(text1, False))

# text = "（1） ddd"
# print(is_alabo_number_prefix(text))

# obj = {
#         "type": "text_line",
#         "id": None,
#         "data": "hahaha",
#     }

# print(list(obj.values()))

# text = "(4)."
# regular_suffix = "$" 
# result = re.findall(f'^[\w\W]+\\u002e{regular_suffix}', text)
# print(result)

result5 = json("static/parse-announcements/2022/filter_by_cash_to_debt_ratio.json")
result = filter_by_receivable_balance(result5)
json("static/parse-announcements/2022/filter_by_receivable_balance.json",result)

# all_industries = _filter(_map(read_table(
#             table_name="stock_basic",
#             fields=["distinct industry"],
#             result_type="dict"
#         ), lambda item: item["distinct industry"]), lambda item: item is not None)
    
# industry_propotion_info_dict = {}
# for industry in all_industries[-2:]:
#     companies = _map(read_table(
#         table_name="stock_basic",
#         fields=["name"],
#         result_type="dict",
#         filter_str=f"where industry = '{industry}'"
#     ), lambda item: item["name"])[:2]

#     industry_file_title_list = []
#     industry_receivable_balance_propotion = []
        
#     for company in companies:
#         file_title = _map(read_table(
#             table_name="announcements",
#             fields=["file_title"],
#             result_type="dict",
#             filter_str=f"where name = '{company}' and title not like '%英文%' and title not like '%取消%' and title not like '%摘要%' and title not like '%公告%' and title not like '%修订前%' and title like '%2022%'"
#             ), lambda item: item["file_title"])
#         if file_title:
#             industry_file_title_list.append(file_title[0])
    
#         generate_announcement(
#         announcement_type=Financial_Statement.合并资产负债表,
#         file_title_list=industry_file_title_list,
#         gen_table=gen_hbzcfzb,
#         use_cache=True,
#         consider_table=False,
# )
#         generate_announcement(
#             announcement_type=Financial_Statement.合并利润表,
#             file_title_list=industry_file_title_list,
#             gen_table=gen_hblrb,
#             use_cache=True,
#             consider_table=False,
#         )
#         if file_title:
#             propotion = receivable_balance_propotion_of_monthly_average_operating_income(file_title[0])
#         industry_receivable_balance_propotion.append(propotion)
#         industry_propotion_info = {
#             industry: industry_receivable_balance_propotion
#         }
#         industry_propotion_info_dict.update(industry_propotion_info)

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
from service.report import caculate_expenses, gen_hblrb, gen_hbzcfzb, gen_zyyw, generate_announcement, get_main_business_income_and_cost, get_management_expense, receivable_balance_propotion_of_monthly_average_operating_income
from strategy.announcements.announcements import filter_by_gross_margin, filter_by_growth_rate_of_management_expense, filter_by_increase_in_accounts_receivable, filter_by_interest_bearing_liabilities, filter_by_ratio_of_expense_and_gross, filter_by_receivable_balance
from utils.index import _filter, _is_empty, _map, find_annotations, get_median, is_alabo_number_prefix, is_chinese_number_prefix, is_period_prefix, json, large_num_format


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

# result5 = json("static/parse-announcements/2021/filter_by_gross_margin.json")
# result = filter_by_receivable_balance(result5)
# json("static/parse-announcements/2021/filter_by_receivable_balance.json",result)

# file_title_list = ["605056__咸亨国际__咸亨国际：2022年年度报告__1216478420"]
# file_title = "605056__咸亨国际__咸亨国际：2022年年度报告__1216478420"
# filter_by_gross_margin(file_title_list)
# r = read_table(
#     table_name="announcements",
#     fields=["file_title"],
#     result_type="dict",
#     filter_str="where title not like '%H股%' and title not like '%意见%' and title not like '%英文%' and title not like '%取消%' and title not like '%摘要%' and title not like '%公告%' and title not like '%修订前%' and title like '%2022%'",
# )
# file_title_list = _map(r, lambda item: item["file_title"]) # ORDER BY RAND() LIMIT 10

# filter_by_interest_bearing_liabilities(file_title_list)
# result1 = json('static/parse-announcements/2022/error/interest_bearing_liabilities.json')
# print(len(result1))
# filter_by_increase_in_accounts_receivable(file_title_list)
# result2 = json('static/parse-announcements/2022/error/increase_in_accounts_receivable.json')
# print(len(result2))

json_ini = [
    ['合并资产负债表'], 
    ['2022年12月31日'], 
    ['（除特别注明外，金额单位均为人民币千元）'], 
    ['项目', '附注七', '期末余额', '上年年末余额'], 
    ['资产：'], 
    ['现金及存放中央银行款项', '1', '8,628,153', '8,634,871'], 
    ['存放同业款项', '2', '1,472,716', '1,602,753'], 
    ['拆出资金', '3', '231,701', '414,815'], 
    ['衍生金融资产', '4', '380,604', '487,760'], 
    ['买入返售金融资产', '-', '-'], 
    ['发放贷款和垫款', '5', '98,711,057', '87,706,952'], 
    ['金融投资：', '6', '55,645,227', '50,842,239'], 
    ['交易性金融资产', '6（1）', '10,595,221', '8,279,591'], 
    ['债权投资', '6（2）', '27,955,018', '24,690,921']
    ]

table_json_format = []
max_length = 4
for item in json_ini:
    index = 0 # 起始索引
    add_list = [] # 长度不一致的补充列表
    # 如果找到附注：在第二列加空字符串
    if len(item)<2:
        item.insert(1,"")
    if find_annotations(json_ini) and len(item)<max_length:
        item.insert(1,"")
    if len(item) < max_length:
        diff = max_length - len(item)
        while index < diff:
            add_list.append("")
            index = index + 1
        item = item + add_list
    table_json_format.append(item)
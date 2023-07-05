# encoding：utf-8
import os
import re
from enum import Enum

import requests
from pdfplumber.table import Table

from db.index import update_table_fields, update_table
from service.report import STATIC_ANNOUNCEMENTS_DIR, download_announcement, download_year_announcements
from utils.index import _map, large_num_format, parse_dataframe, print_dataframe, _map2, list2dict, add_date, add_date_str, str2date, \
    get_current_date, replace_nan_from_dataframe, _is_nan, get_path, _is_empty, get_dict_key_by_index, txt, mul_str, \
    has_chinese_number, _find, json, _dir
from utils.stock import fq, _filter

# '600276.SH', '600276', '恒瑞医药', '江苏', '化学制药', '主板', 'L', datetime.date(2000, 10, 18)
# '002475.SZ', '002475', '立讯精密', '深圳', '元器件', '主板', 'L', datetime.date(2010, 9, 15)
# '688126.SH', '688126', '沪硅产业', '上海', '半导体', '科创板', 'L', datetime.date(2020, 4, 20)
# '600612.SH', '600612', '老凤祥', '上海', '服饰', '主板', 'L', datetime.date(1992, 8, 14)
# '600000.SH', '600000', '浦发银行', '上海', '银行', '主板', 'L', datetime.date(1999, 11, 10), None, 'H'


# row_list = read_table('d_688126', result_type='dict', filter_str="WHERE trade_date <= date'20201130'")
#
# adj_factors = _map(row_list, lambda row: row['adj_factor'])
#
# avg_af = numpy.mean(adj_factors)
# mid_af = numpy.median(adj_factors)
# target_af = adj_factors[-5:]
# print(avg_af, mid_af, target_af)

# test_list = [1,3,4,5,7,9,10,100]
# avg = numpy.mean(test_list)
# mid = numpy.median(test_list)
#
# print(avg,mid)

# print("哈哈".startswith("ST"))

# total = get_total('stock_basic', filter_str="WHERE `name` like '%药%'")
# print(total)

# print(format_code("000001.SZ", _to="bs"))
# fetch_stock_basic_from_bs()

import pdfplumber
import pandas as pd


# read_path = 'reports/lxjm.pdf'
# kw1 = "公允反映了"
# kw2 = "标准无保留意见"
# pdf_content = pdfplumber.open(read_path)
# result_df = pd.DataFrame()
# for page in pdf_content.pages:
#     table = page.extract_table()
#     df_detail = pd.DataFrame(table)
#     content = page.extract_text_simple(x_tolerance=3, y_tolerance=3)
#     if kw1 in content or kw2 in content:
#         print(1)































GAP = 17











def amount_empty_to_zero(value: str | float):
    if value is None or value == '':
        return 0
    return value


def calculate_interest_bearing_liabilities(hbzcfzb_json):
    '''
        计算有息负债
    '''
    interest_items = ["短期借款", "交易性金融负债", "长期借款", "应付债券", "一年内到期的非流动负债"]
    interest_bearing_liabilities_current_list = []
    interest_bearing_liabilities_current_list_new = []
    for row in hbzcfzb_json:
        result = _map(row,lambda item: large_num_format(item))
        if result[0] in interest_items:
            interest_bearing_liabilities_current_list.append(result[1])
    for item in interest_bearing_liabilities_current_list:
        item = 0 if _is_empty(item) else item
        interest_bearing_liabilities_current_list_new.append(item)
    interest_bearing_liabilities_current_list_new = _map(interest_bearing_liabilities_current_list_new, lambda x: float(x))
    interest_bearing_liabilities_current = sum(interest_bearing_liabilities_current_list_new)
   
    return interest_bearing_liabilities_current


def get_total_assets(hbzcfzb_json):
    fields = _map(hbzcfzb_json, lambda item: item[0])
    key_word = _filter(fields, lambda field: field.replace('\n', '').replace('（', '').replace('）',
                                                                                         '') in ["负债和所有者权益或股东权益总计", "负债和所有者权益总计"])
    for row in hbzcfzb_json:
        if row[0] in key_word:
            row[1] = float(large_num_format(row[1]))
    return row[1]
        
def caculate_interest_bearing_liabilities_rate(interest_bearing_liabilities, total_assets):
    interest_bearing_liabilities_rate = interest_bearing_liabilities / total_assets 
    print("有息负债比为：", interest_bearing_liabilities_rate*100,"%")
    return interest_bearing_liabilities_rate * 100








# all_tables = json('/all_tables.json')
# table = _filter(all_tables, lambda table: "合并资产负债表" in table["desc"]["top"])
# txt('/target.txt', table)


# parse_pdf('./reports/hgcy.pdf')

# pdf_names = _dir("/reports", ['pdf'])
# for pdf_name in pdf_names:
#     print(parse_pdf(pdf_name))
# pdfs = [
#     "000001__平安银行__2006年年度报告__21676577.pdf",
#     "688126__沪硅产业__2019年年度报告__1207650684.pdf"
# ]
# result = parse_pdf(pdfs[1])
# print(result)

# download_announcement('http://static.cninfo.com.cn/finalpage/2011-03-18/59136299.PDF', title="test")
# url = "http://static.cninfo.com.cn/finalpage/2011-03-18/59136299.PDF"
# url = "http://static.cninfo.com.cn/finalpage/2007-03-22/21676577.PDF"
# response = requests.get(url)
#
# f = open("test.pdf", "wb")
# f.write(response.content)

# parse_pdf("hgcy.pdf")

# file_size = os.path.getsize("hgcy.pdf")
# print(file_size)

# download_year_announcements()

# update_table_fields('announcements', add_field_defines=[
#     "`disabled` TINYINT(1) DEFAULT 0"
# ])

# update_table("announcements", ['`disabled`'], [0], f"WHERE `symbol`='000721'")

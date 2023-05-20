import pandas
from pandas import DataFrame, concat
from decimal import Decimal, getcontext
from datetime import date, timedelta, datetime

from db.str import safe_column, safe_field, safe_field_define
from ts.index import pro_bar, pro_api, fetch_daily, format_fields, fields_map_df
from db.index import show_tables, delete_table, create_table, insert_table, read_table, describe_table, copy_table, \
    clear_table, update_table_fields, get_last_row, get_first_row
from model.index import describe_json
from model.model import TableModel
from utils.index import _map, parse_dataframe, print_dataframe, _map2, list2dict
from utils.stock import fq, _filter, fq2
from service.index import api_query, get_current_d_tables

import baostock as bs

# '600276.SH', '600276', '恒瑞医药', '江苏', '化学制药', '主板', 'L', datetime.date(2000, 10, 18)
# '002475.SZ', '002475', '立讯精密', '深圳', '元器件', '主板', 'L', datetime.date(2010, 9, 15)
# '688126.SH', '688126', '沪硅产业', '上海', '半导体', '科创板', 'L', datetime.date(2020, 4, 20)
# '600612.SH', '600612', '老凤祥', '上海', '服饰', '主板', 'L', datetime.date(1992, 8, 14)
# '600000.SH', '600000', '浦发银行', '上海', '银行', '主板', 'L', datetime.date(1999, 11, 10), None, 'H'

# 两个任务：1. 比较未复权数据准确性 2. 如果未复权数据准确，则根据我们的算法算出前复权数据，再进行比较准确性

# ts_codes = ['688072.SH', '601288.SH', '600276.SH', '002475.SZ']
# start_date = '20200101'
# end_date = '20230517'
#
# test_dates = ['20200303', '20210407', '20220909', '20230517']
# #
# #
# ts_code = '600276.SH'
# symbol = ts_code.split('.')[0]
#
#
# def optimize_result(result):
#
#     return result
#
#
# result = read_table('d_600276')
# result = optimize_result(result)
# # 经过修正，输出新的close_qfq
# test_column_names = ['date', 'close_qfq']
# test = [('20060403', '-0.27'), ('20110913', '4.53'), ('20160126', '12.51'), ('20190704', '45.42'),
#         ('20220628', '35.50'), ('20220701', '38.78')]
# test_times = _map(test, lambda item: item[0])
# test_df = DataFrame(columns=test_column_names, data=test)
#
# print(test_df)
#
# jack = _filter(result, lambda row: date.strftime(row[1], '%Y%m%d') in test_times)
# jack = _map(jack, lambda item: (date.strftime(item[1], '%Y%m%d'), item[-1]))
# jack_df = DataFrame(columns=test_column_names, data=jack)
# print(jack_df)
#
# r = []
# for index, item in enumerate(jack):
#     test_item = test[index]
#     gap = Decimal(str(item[1])) - Decimal(str(test_item[1]))
#     r.append((item[0], str(gap)))
#
# print(r)

# [('20060403', '0.991593'), ('20110913', '0.68664'), ('20160126', '0.5258'), ('20190704', '0.0952'), ('20220628', '0.0214'), ('20220701', '0.0087')]

#
# ts_code = '300001.SZ'
# df = fetch_daily(ts_code)
#
# df = fq2(df)
# cols, values = parse_dataframe(df)
# desc = describe_json('d')
# clear_table('d_300001')
# insert_table('d_300001', desc.safe_column_names, values)
#
#
# lg = bs.login()
# 查询2015至2017年复权因子
# , start_date="2015-01-01", end_date="2017-12-31"
# rs_list = []
# rs_factor = bs.query_adjust_factor(code="sz.300001")
# while (rs_factor.error_code == '0') & rs_factor.next():
#     rs_list.append(rs_factor.get_row_data())
# result_factor = pandas.DataFrame(rs_list, columns=rs_factor.fields)
# # 打印输出
# print(result_factor)
# lg = bs.login()
# desc = describe_json('d')
# print('ts_fields', desc.column_names)
# bs_fields = format_fields(desc.column_names, _from='ts', _to='bs')
# print('bs_fields', bs_fields)
# rs = bs.query_history_k_data_plus("sz.300001", ",".join(bs_fields),start_date='2015-02-01', end_date='2015-02-01', adjustflag="2")
# print(rs.data)
#
# #### 打印结果集 ####
# data_list = []
# while (rs.error_code == '0') & rs.next():
#     # 获取一条记录，将记录合并在一起
#     data_list.append(rs.get_row_data())
# bs_result = DataFrame(data_list, columns=rs.fields)
#
# #### 结果集输出到csv文件 ####
# bs_result.to_csv("./history_A_stock_k_data_qfq.csv", index=False)
# print(bs_result)
#
# df1 = pro_api.daily(ts_code='300001.SZ', start_date='20150202', end_date='20150202')
# print(df1)
# ts_code = '600519.SH'
# df_ts = fetch_daily(ts_code)
# df_ts = fq(df_ts)
#
# lg = bs.login()
# desc = describe_json('d')
# bs_fields = format_fields(desc.column_names, _from='ts', _to='bs')
# rs = bs.query_history_k_data_plus("sh.600519", ",".join(bs_fields), start_date='2001-08-27',
#                                   adjustflag="2")
# for row in rs.data:
#     row[1] = row[1].replace("-", "")
#     print(row[1])
#
# data_list = []
# while (rs.error_code == '0') & rs.next():
#     # 获取一条记录，将记录合并在一起
#     data_list.append(rs.get_row_data())
# df_bs = DataFrame(data_list, columns=rs.fields)
#
#
# #### 结果集输出到csv文件 ####
# df_bs.to_csv("./history_A_stock_k_data_qfq_bs.csv", index=False)
# df_ts.to_csv("./history_A_stock_k_data_qfq_ts.csv", index=False)
# update_table_fields("d_000002", "trade_date")
#
# update_table_fields(
# "d_000001",
# add_field_defines=["shareholder CHAR(10) NOT NULL", "hold_per FLOAT(10) NOT NULL"],
# delete_fields=["close_qfq", "low_qfq"],
# update_field_defines={"shareholder": "share_holder"}
# )

# d = {"a":1, "b":2, "c": 3}
# r = _map(d, lambda item: d.get(item))

# arr = "jack PRIMARY KEY CHAR(88888)".split(" ")
# print(len(arr))

# print(safe_field_define("`share_holder` char(10) NOT NULL"))
# 
# result = get_first_row('d_000002', fields=['trade_date'], order_by='trade_date')
# print(result['trade_date'])




import time

import numpy
import pandas
import numpy as np
import pandas as pd
from pandas import DataFrame, concat
from decimal import Decimal, getcontext
from datetime import date, timedelta, datetime

from db.str import safe_column, safe_field, safe_field_define
from ts.index import pro_bar, pro_api, fetch_daily, format_fields, fields_map_df
from db.index import show_tables, delete_table, create_table, insert_table, read_table, describe_table, copy_table, \
    clear_table, update_table_fields, get_last_row, get_first_row, get_total
from model.index import describe_json
from model.model import TableModel
from utils.index import _map, parse_dataframe, print_dataframe, _map2, list2dict, add_date, add_date_str, str2date, \
    get_current_date, replace_nan_from_dataframe, _is_nan, get_path, _is_empty
from utils.stock import fq, _filter
from service.index import api_query, get_current_d_tables, get_ts_code_from_symbol, update_d_tables

import baostock as bs

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

total = get_total('stock_basic', filter_str="WHERE `name` like '%药%'")
print(total)
import datetime
import pandas as pd
from service.index import create_new_d_tables, get_current_d_tables, api_query, refresh_table_stock_basic, \
    format_table_row
from utils.index import get_diff, get_diff2, _set, _map, _find
from db.index import read_table

# refresh_table_stock_basic()


# list1 = [[1], [2], 3]
# list2 = [4, 5, [2], (1,)]
#
# print(get_diff2(list1, list2, lambda v1, v2: _set(v1) == _set(v2)))

# [[1]] [[2]] [[3]] => [[1],[2],[3]]

# live_stocks = [[1,11,111]]
# dead_stocks = [[2]]
# pause_stocks = [[3]]
#
# result = live_stocks + dead_stocks + pause_stocks
#
# print(result)

# new_stocks = [["平安", "D"], ["N华园", "L"]]
# old_stocks = (("平安", "L"),)
#
# print(get_diff2(new_stocks, old_stocks, lambda v1, v2: _set(v1) == _set(v2)))

# refresh_table_stock_basic()

table_name = 'stock_basic'
# new_stocks = [api_query(table_name, list_status='L')[1][0]]
target = _find(read_table(table_name), lambda item: item[0] == '838837.BJ')
print(target)
# old_format_stocks = _map(old_stocks, lambda row: format_table_row(row))
# print('new_stocks', len(new_stocks), new_stocks, '\n')
# print('old_stocks', len(old_stocks), old_stocks, '\n')
# print('old_format_stocks', len(old_format_stocks), old_format_stocks, '\n')
#
# print(get_diff2(new_stocks, old_format_stocks))
# date_value = datetime.date(1991, 4, 3)
# print(date_value.strftime("%Y%m%d"))
# print(type(_set(date_value)))

# ts_code = '838837.BJ'
# result = api_query('stock_basic', ts_code=ts_code)[1]
# print(result)

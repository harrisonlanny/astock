# from ts.index import pro_api, pro_bar
from utils.index import _map
#
# ts_code = '300750.SZ'
# start_date = '20230510'
# end_date = '20230512'
# data1 = pro_api.query('daily', ts_code=ts_code, start_date=start_date, end_date=end_date)
# data2 = pro_bar(freq='D', ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date)
# data3 = pro_bar(freq='D', ts_code=ts_code, adj='hfq', start_date=start_date, end_date=end_date)
#
#
# c1, v1 = parse_dataframe(data1)
# c2, v2 = parse_dataframe(data2)
# c3, v3 = parse_dataframe(data3)
#
# print(v1)
# print('\n')
# print(v2)
# print('\n')
# print(v3)

from db.index import read_table, create_table, clear_table
from model.index import get_columns_info
from utils.index import is_iterable

# values = read_table('stock_basic', ('symbol', ))
# values = _map(values, lambda tuple_item: tuple_item[0])
#
# for symbol in values:
#     table_name = f"d_{symbol}"

# (columns,a,b) = get_columns_info('stock_basic')
# print(columns)

# clear_table('d_2222')
from service.index import refresh_table
from db.index import show_tables, has_tables
from utils.index import is_subset

# print(is_table_exits('stock_basic'))

# refresh_table('stock_basic')

# print(is_subset([1,2,3,4,5, "a"], [2,3,1,"a"]))
print(has_tables(['stock_basic']))

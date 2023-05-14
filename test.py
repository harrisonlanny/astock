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

from db.index import read_table, create_table, clear_table,show_tables
from model.index import get_columns_info
from utils.index import is_iterable
from service.index import create_new_d_tables

# create_new_d_tables()
print(show_tables())

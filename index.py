from model.index import get_column_name_list, get_columns
from ts.index import pro_api

# 获取定义的columns结构 （不是从数据库，而是从db/tables.json）
columns = get_columns('stock_basic')
column_name_list = get_column_name_list(columns)
column_name_list_str = ','.join(column_name_list)

print('stock_basic column_name_list: ', column_name_list_str)

live_stocks = pro_api.query('stock_basic', list_status='L', fields=column_name_list_str)
dead_stocks = pro_api.query('stock_basic', list_status='D', fields=column_name_list_str)

print('live_stocks')
print(live_stocks)
print('\n')
print('dead_stocks')
print(dead_stocks)

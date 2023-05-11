from model.index import get_column_name_list, get_columns
from ts.index import pro_api
from db.index import insert_table,delete_table,create_table

# 获取定义的columns结构 （不是从数据库，而是从db/tables.json）
columns = get_columns('stock_basic')
column_name_list = get_column_name_list(columns)
column_name_list_str = ','.join(column_name_list)

print('stock_basic column_name_list: ', column_name_list)
#
# live_stocks = pro_api.query('stock_basic', list_status='L', fields=column_name_list_str)
# dead_stocks = pro_api.query('stock_basic', list_status='D', fields=column_name_list_str)
#
# print('live_stocks')
# print(live_stocks)
# print('\n')
# print('dead_stocks')
# print(dead_stocks)

insert_table('stock_basic', column_name_list, (
    ('000001.SZ', '000001', '祝你平安', '成都', '婴幼儿', '创业板', 'L', '20230511', '20230511', 'S'),
    ('000002.SZ', '000002', '哈哈渣男', '成都', '婴幼儿', '创业板', 'L', '20230512', '20230512', 'S'),
))

# delete_table('stock_basic')
# create_table('stock_basic', columns)

# value = 'abc'
#
# print(str)


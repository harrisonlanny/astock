from model.index import get_columns_info
from ts.index import pro_api
from db.index import insert_table, delete_table, create_table
from utils.index import parse_dataframe

# 获取定义的columns结构 （不是从数据库，而是从db/tables.json）
columns, column_name_list, column_name_list_str = get_columns_info('stock_basic')

print('stock_basic column_name_list: ', column_name_list)
#
# live_stocks = pro_api.query('stock_basic', list_status='L', fields=column_name_list_str)
dead_stocks = pro_api.query('stock_basic', list_status='D', fields=column_name_list_str)

dead_stocks_column_name_list, dead_stock_values = parse_dataframe(dead_stocks)

delete_table('stock_basic')
create_table('stock_basic', columns)

insert_table('stock_basic', column_name_list, dead_stock_values)

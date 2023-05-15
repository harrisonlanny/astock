from utils.index import _filter, _map, _find_index, _find
from db.index import show_tables, delete_table, create_table, insert_table, read_table, describe_table
from service.index import get_new_symbols, refresh_table_stock_basic, create_new_d_tables, get_columns_info
import re

#
# ddtables = _filter(show_tables(), lambda item: item.startswith('d_d_'))
#
# for table in ddtables:
#     delete_table(table)

# ddtables = _filter(show_tables(), lambda item: item.startswith('d_d_'))
# print(ddtables)

# refresh_table_stock_basic()
# create_new_d_tables()

# print(show_tables())

# copy_table('stock_basic', 'test_stock_basic')
# print(read_table('stock_basic'))


describe = describe_table('stock_basic')
print(describe.safe_column_names)




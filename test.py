from utils.index import _filter
from db.index import show_tables,delete_table
from service.index import get_new_symbols,refresh_table_stock_basic,create_new_d_tables
#
# ddtables = _filter(show_tables(), lambda item: item.startswith('d_d_'))
#
# for table in ddtables:
#     delete_table(table)

# ddtables = _filter(show_tables(), lambda item: item.startswith('d_d_'))
# print(ddtables)

refresh_table_stock_basic()
create_new_d_tables()



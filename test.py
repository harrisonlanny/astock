from db.index import show_tables, delete_table, create_table, insert_table, read_table, describe_table, copy_table
from model.index import get_columns
from model.model import TableModel
from utils.index import _map

# copy_table('stock_basic', 'test_stock_basic')
#
# result = describe_table('stock_basic')
# print(result.column_names)
# t = TableModel(columns=get_columns('stock_basic'))
# print(t.primary_key)

insert_table('stock_basic', describe_table('stock_basic').column_names, [])

from db.index import create_table, describe_table
from model.index import describe_json
from service.index import refresh_table_stock_basic, create_new_d_tables, delete_d_tables, get_current_d_tables, \
    update_d_tables,clear_current_d_tables

clear_current_d_tables()
refresh_table_stock_basic()
create_new_d_tables()
update_d_tables(sleep_time=0.2, update_st=False)
print('成功！！')

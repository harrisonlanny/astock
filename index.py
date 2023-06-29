from db.index import create_table, describe_table
from model.index import describe_json
from service.index import refresh_table_stock_basic, create_new_d_tables, delete_d_tables, get_current_d_tables, \
    update_d_tables
from service.report import refresh_table_announcements, download_year_announcements

# refresh_table_stock_basic()
# create_new_d_tables()


update_d_tables(sleep_time=0.2, update_st=False)
# refresh_table_announcements(sleep_time=0.25)

# download_year_announcements()
#
# print('成功！！')

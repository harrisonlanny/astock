from db.index import delete_table, create_table, insert_table
from model.index import get_columns_info
from ts.index import pro_api
from utils.index import parse_dataframe


def refresh_table_stock_basic(table_name='stock_basic'):
    # 获取model 表结构
    columns, column_name_list, column_name_list_str = get_columns_info(table_name)
    # 从tushare获取数据
    df = pro_api.query(table_name,  fields=column_name_list_str)
    # 将dataframe转成通用格式
    _columns, data = parse_dataframe(df)
    delete_table(table_name)
    create_table(table_name, columns)
    insert_table(table_name, column_name_list, data)


# def refresh_table(table_name):
#     # 获取model 表结构
#     columns, column_name_list, column_name_list_str = get_columns_info(table_name)
#     # 从tushare获取数据
#     data = pro_api.query(table_name,  fields=column_name_list_str)
#
#     #
#     delete_table(table_name)
#     create_table(table_name, columns)
#     insert_table(table_name, column_name_list, data)

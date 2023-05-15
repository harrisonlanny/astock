'''
Created on 2023年5月11日

@author: Harrison
'''

import pymysql
import pandas as pd
import numpy as np
from utils.index import _map, _safe_join, add_single_quotation, none_to_null_str, is_iterable, is_subset
# from model.index import add_fyh_for_column_name

from constants import DATABASE_NAME

db = pymysql.connect(host='localhost',
                     user='root',
                     password='HaiHai211!',
                     database=DATABASE_NAME)


def sql(sqls, callback_after_execute=None):
    cursor = db.cursor()
    result = None
    for sql in sqls:
        cursor.execute(sql)
    if callback_after_execute:
        result = callback_after_execute(cursor)
    cursor.close()
    return result


# def is_table_exits(table_name):
#     result = sql(
#         [
#             f"SELECT TABLE_NAME  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{DATABASE_NAME}' AND TABLE_NAME = '{table_name}'"],
#         lambda cursor: cursor.fetchall()
#     )
#     return len(result) > 0

def show_tables():
    return _map(sql([f"show tables"], lambda cursor: cursor.fetchall()), lambda tuple_item: tuple_item[0])


def has_tables(table_name_list: list):
    tables = show_tables()
    return is_subset(tables, table_name_list)


def delete_table(table_name):
    sql([f"DROP TABLE IF EXISTS {table_name}"])


def create_table(table_name, safe_columns):
    sql([
        # f"DROP TABLE IF EXISTS {table_name}",
        f"CREATE TABLE {table_name} ({','.join(safe_columns)}) CHARSET=utf8"
    ])


def clear_table(table_name):
    delete_sql = f"DELETE FROM {table_name}"
    print(delete_sql)
    sql([
        delete_sql
    ], lambda cursor: db.commit())


# row_list: [[1,2,3],[4,5,6]] => '(1,2,3)','(4,5,6)'
# row: [1,2,3] => '1,2,3'
def insert_table(table_name, column_name_list, row_list):
    row_list_str = ','.join(
        _map(row_list,
             lambda row: f'({_safe_join(_map(row, lambda col_v: none_to_null_str(add_single_quotation(col_v))))})'))

    insert_sql = f"INSERT INTO {table_name} ({','.join(column_name_list)}) VALUES {row_list_str}"
    print('insert sql', insert_sql)

    sql([
        insert_sql
    ], lambda cursor: db.commit())


def read_table(table_name: str, read_columns: list = None):
    read_columns_str = '*'
    if is_iterable(read_columns) and len(read_columns) > 0:
        read_columns_str = ','.join(read_columns)

    read_sql = f"SELECT {read_columns_str} FROM {table_name}"
    return sql([read_sql], lambda cursor: cursor.fetchall())


if __name__ == '__main__':
    table_name = 'haha'
    columns = ['a', 'b', 'c']
    print(f"CREATE TABLE {table_name} ({','.join(columns)}) CHARSET=utf8")

# 创建游标对象
# cursor = db.cursor()
# 查询ts_code列表
# ts_code = "SELECT ts_code FROM stock_basic"
# ts_lst = []
# cursor.execute(ts_code)  # 执行sql语句，也可执行数据库命令，如：show tables
# result = cursor.fetchall()  # 所有结果
# for ts in result:
#     ts = ts[0]
#     ts_lst.append(ts)


# cursor.close()  # 关闭当前游标
# db.close()  # 关闭数据库连接

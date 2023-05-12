'''
Created on 2023年5月11日

@author: Harrison
'''

import pymysql
import pandas as pd
import numpy as np
from utils.index import _map, _safe_join, add_single_quotation,none_to_null_str

db = pymysql.connect(host='localhost',
                     user='root',
                     password='HaiHai211!',
                     database='astock')


def sql(sqls, callback_after_execute=None):
    cursor = db.cursor()
    for sql in sqls:
        cursor.execute(sql)
    if callback_after_execute:
        callback_after_execute()
    cursor.close()


def delete_table(table_name):
    sql([f"DROP TABLE IF EXISTS {table_name}"])


def create_table(table_name, columns):
    sql([
        # f"DROP TABLE IF EXISTS {table_name}",
        f"CREATE TABLE {table_name} ({','.join(columns)}) CHARSET=utf8"
    ])


# row_list: [[1,2,3],[4,5,6]] => '(1,2,3)','(4,5,6)'
# row: [1,2,3] => '1,2,3'
def insert_table(table_name, column_name_list, row_list):
    row_list_str = ','.join(
        _map(row_list, lambda row: f'({_safe_join(_map(row, lambda col_v: none_to_null_str(add_single_quotation(col_v))))})'))

    insert_sql = f"INSERT INTO {table_name} ({','.join(column_name_list)}) VALUES {row_list_str}"
    print('insert sql', insert_sql)

    sql([
        insert_sql
    ], db.commit)


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

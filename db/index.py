'''
Created on 2023年5月11日

@author: Harrison
'''

import pymysql
import pandas as pd
import numpy as np
from utils.index import _map, _safe_join, add_single_quotation, none_to_null_str, is_iterable, is_subset, _find_index, \
    get_diff, _filter
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


def get_table_primary_key(table_name):
    get_sql = f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'"
    # print(get_sql)
    rows = sql([get_sql], lambda cursor: cursor.fetchall())
    if len(rows) == 0:
        return None
    # 4是Column_name的index
    return rows[0][4]


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
    return insert_sql


def column_row_match(column_name_list, row):
    update_sqls = []
    for index in range(0, len(column_name_list)):
        update_fields = f"{column_name_list[index]} = {row[index]}"
        update_sqls.append(update_fields)
    return ",".join(update_sqls)


def update_table(table_name, column_name_list, row, conditions):
    update_fields = column_row_match(column_name_list, row)
    update_sql = f"UPDATE {table_name} SET {update_fields} {conditions}"
    print('update sql', update_sql)
    # TODO FINISH REAL COMMIT
    # sql([
    #     update_sql
    # ], lambda cursor: db.commit())
    # return update_sql


def insert_update_table(table_name, column_name_list, row_list):
    primary_key = get_table_primary_key(table_name)
    print(f"{table_name}的主键是：{primary_key}", '\n')
    if primary_key is None:
        raise Exception(f"{table_name} 表不存在主键!")
    if primary_key not in column_name_list:
        raise Exception(f"column_name_list中必须包含主键{primary_key}!")

    # 从表中读取所有的主键values
    # 以stock_basic表为例，pk_values = ['000001.SZ','000002.SZ',....]
    # 因为我们只取[primary_key]，所以row[0]即能拿到primary_key对应的value
    old_ids: list[str] = _map(read_table(table_name, [primary_key]), lambda row: row[0])
    pk_index: int = _find_index(column_name_list, lambda cname: cname == primary_key)
    new_ids: list[str] = _map(row_list, lambda row: row[pk_index])

    # new_ids: [1,2,5]
    # old_ids: [1,2,3,4]
    # insert_ids: [5]  update_ids: [1,2]
    insert_ids = get_diff(new_ids, old_ids)
    update_ids = get_diff(new_ids, insert_ids)

    print('insert_ids', insert_ids)
    print('update_ids', update_ids)

    if len(insert_ids):
        insert_values = _filter(row_list, lambda row: row[pk_index] in insert_ids)
        insert_table(table_name, column_name_list, insert_values)

    if len(update_ids):
        update_values = _filter(row_list, lambda row: row[pk_index] in update_ids)
        for row in update_values:
            update_table(table_name, column_name_list, row, f"WHERE {primary_key} = {row[pk_index]}")


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

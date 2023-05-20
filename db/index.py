'''
Created on 2023年5月11日

@author: Harrison
'''
import datetime

import numpy
import pymysql

from db.str import safe_column, safe_field, safe_field_define, is_field_define, get_field_desc_from_define
from utils.index import _map, _safe_join, add_single_quotation, none_to_null_str, is_iterable, is_subset, _find_index, \
    get_diff, _filter, _find, _map2, list2dict
from model.model import TableModel
from constants import DATABASE_NAME, DATE_FORMAT, FIELDS_DDL

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


def show_tables():
    return _map(sql([f"show tables"], lambda cursor: cursor.fetchall()), lambda tuple_item: tuple_item[0])


def has_tables(table_name_list: list):
    tables = show_tables()
    return is_subset(tables, table_name_list)


def delete_table(table_name):
    sql([f"DROP TABLE IF EXISTS {table_name}"])


def clear_table(table_name):
    delete_sql = f"DELETE FROM {table_name}"
    print(delete_sql)
    sql([delete_sql], lambda cursor: db.commit())


def describe_table(table_name):
    # return sql([f"describe {table_name}"], lambda cursor: cursor.fetchall())
    # (('stock_basic', 'CREATE TABLE `stock_basic` (\n  `ts_code` char(9) NOT NULL,\n  `symbol` char(6) NO ....
    describe: str = sql([f"SHOW CREATE TABLE {table_name}"], lambda cursor: cursor.fetchall())[0][1]
    return TableModel(describe)


def copy_table(from_name, to_name):
    describe = describe_table(from_name)
    delete_table(to_name)
    create_table(to_name, describe.safe_columns)
    insert_table(to_name, describe.safe_column_names, read_table(from_name))


def create_table(table_name, safe_columns):
    sql([
        # f"DROP TABLE IF EXISTS {table_name}",
        f"CREATE TABLE {table_name} ({','.join(safe_columns)}) CHARSET=utf8"
    ])


def get_table_primary_key(table_name):
    get_sql = f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'"
    # print(get_sql)
    rows = sql([get_sql], lambda cursor: cursor.fetchall())
    if len(rows) == 0:
        return None
    # 4是Column_name的index
    return rows[0][4]


def format_insert_value(value):
    if isinstance(value, datetime.date):
        value = value.strftime('%Y-%m-%d')
    if isinstance(value, numpy.nan):
        value = None
    value = none_to_null_str(add_single_quotation(value))
    return value


# row_list: [[1,2,3],[4,5,6]] => '(1,2,3)','(4,5,6)'
# row: [1,2,3] => '1,2,3'
def insert_table(table_name, column_names: list[str], row_list):
    safe_column_names = _map(column_names, lambda cname: f"`{cname.replace('`', '')}`")

    row_list_str = ','.join(
        _map(row_list,
             lambda row: f'({_safe_join(_map(row, lambda col_v: format_insert_value(col_v)))})'))

    insert_sql = f"INSERT INTO {table_name} ({','.join(safe_column_names)}) VALUES {row_list_str}"
    # print('insert sql', insert_sql)

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


# 'ts_code' field
# 'ts_code CHAR(9) PRIMARY KEY' field_define

# 1. add fields
# 2. update fields
# 3. delete fields

# action: 'ADD' | 'update' | 'DELETE'
def update_table_fields(
        table_name: str,
        add_field_defines: list[str] = None,
        update_field_defines: dict[str, str] = None,
        delete_fields: list[str] = None,
):
    add_field_sqls = _map(add_field_defines,
                          lambda field_define: f"ALTER TABLE {table_name} ADD COLUMN {safe_field_define(field_define)}")
    delete_field_sqls = _map(delete_fields, lambda field: f"ALTER TABLE {table_name} DROP COLUMN {safe_field(field)}")

    desc = None
    real_update_field_defines = {}
    for from_field in update_field_defines:
        to_field_define = update_field_defines.get(from_field)
        if not is_field_define(to_field_define):
            if desc is None:
                desc = describe_table(table_name)
            from_field_define = _find(desc.columns, lambda field_define: field_define.split(' ')[0] == from_field)
            field_desc = get_field_desc_from_define(from_field_define)
            update_field_define = safe_field(to_field_define) + " " + field_desc
        else:
            update_field_define = safe_field_define(to_field_define)
        real_update_field_defines[from_field] = update_field_define

    update_field_sqls = _map(real_update_field_defines, lambda
        field: f"ALTER TABLE {table_name} CHANGE {safe_field(field)} {safe_field_define(real_update_field_defines.get(field))}")

    sql_list = add_field_sqls + delete_field_sqls + update_field_sqls
    print(sql_list)
    sql(sql_list)


def insert_update_table(table_name, column_names, row_list):
    describe = describe_table(table_name)
    # primary_key = get_table_primary_key(table_name)
    print(f"{table_name}的主键是：{describe.primary_key}", '\n')
    if describe.primary_key is None:
        raise Exception(f"{table_name} 表不存在主键!")
    if describe.primary_key not in column_names:
        raise Exception(f"column_name_list中必须包含主键{describe.primary_key}!")

    # 从表中读取所有的主键values
    # 以stock_basic表为例，pk_values = ['000001.SZ','000002.SZ',....]
    # 因为我们只取[primary_key]，所以row[0]即能拿到primary_key对应的value
    old_ids: list[str] = _map(read_table(table_name, [describe.primary_key]), lambda row: row[0])
    pk_index: int = _find_index(column_names, lambda cname: cname == describe.primary_key)
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
        insert_table(table_name, column_names, insert_values)

    if len(update_ids):
        update_values = _filter(row_list, lambda row: row[pk_index] in update_ids)
        for row in update_values:
            update_table(table_name, column_names, row, f"WHERE {describe.primary_key} = {row[pk_index]}")


def read_table(table_name: str, fields: list[str] = None, filter_str='', result_type: str = 'list'):
    read_columns_str = '*'
    if is_iterable(fields) and len(fields) > 0:
        read_columns_str = ','.join(fields)
    read_sql = f"SELECT {read_columns_str} FROM {table_name} {filter_str}"
    row_list = sql([read_sql], lambda cursor: cursor.fetchall())
    if result_type == 'dict':
        if read_columns_str == '*':
            desc = describe_table(table_name)
            fields = desc.column_names
        return _map(row_list, lambda row: list2dict(fields, row))
    # TODO elif result_type == 'df':

    return row_list


def get_first_row(table_name: str, fields: list[str] = None, order_by: str = '', result_type: str = 'dict'):
    result = read_table(table_name, fields, filter_str=f"ORDER BY {order_by} LIMIT 1", result_type=result_type)
    if len(result):
        return result[0]
    return None


def get_last_row(table_name: str, fields: list[str] = None, order_by: str = '', result_type: str = 'dict'):
    result = read_table(table_name, fields, filter_str=f"ORDER BY {order_by} DESC LIMIT 1", result_type=result_type)
    if len(result):
        return result[0]
    return None


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

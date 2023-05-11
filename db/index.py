'''
Created on 2023年5月11日

@author: Harrison
'''

# import pandas as pd
import pymysql

db = pymysql.connect(host='localhost',
                     user='root',
                     password='HaiHai211!',
                     database='astock')


def sql(sqls):
    cursor = db.cursor()
    for sql in sqls:
        cursor.execute(sql)
    cursor.close()


def create_table(table_name, columns):
    sql([
        f"DROP TABLE IF EXISTS {table_name}",
        f"CREATE TABLE {table_name} ({','.join(columns)}) CHARSET=utf8"
    ])


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


if __name__ == '__main__':
    create_table('stock_basic', [
        'ts_code CHAR(9) PRIMARY KEY',
        'symbol MEDIUMINT(6) UNSIGNED NOT NULL',
        ''
    ])

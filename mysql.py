'''
Created on 2020年1月30日

@author: JM
'''
import time

import pandas as pd
import tushare as ts
import pymysql

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

ts.set_token('f3d6e975a07792eeb2978aed05fc7c4b31d408287d74ab0daf6a4464')
# db_conn = pymysql.connect(
#     host='localhost',
#     port=3306,
#     user='root',
#     password='HaiHai211!',
#     database='astock',
#     charset='utf8'
# )
engine = create_engine('mysql+pymysql://root:HaiHai211!@127.0.0.1:3306/astock?charset=utf8&use_unicode=1')
Session = sessionmaker(bind=engine)
session = Session()
con = engine.connect()


# def read_data():
#     sql = """SELECT * FROM stock_basic LIMIT 20"""
#     df = pd.read_sql_query(sql, engine_ts)
#     return df


def write_data(df, table_name):
    res = df.to_sql(table_name, con, index=False, if_exists='append', chunksize=5000)
    # print(res)


def get_data():
    pro = ts.pro_api()
    df = pro.stock_basic()
    return df


# 获取十大流通股东
def get_shareholder(ts_code):
    pro = ts.pro_api()
    year = time.strftime("%Y", time.localtime())
    month = time.strftime("%m", time.localtime())
    if month in ["01", "02", "03"]:
        year = year - 1
        month = "12"
        end_day = "31"
    elif month in ["04", "05", "06"]:
        month = "03"
        end_day = "31"
    elif month in ["07", "08", "09"]:
        month = "06"
        end_day = "30"
    else:
        month = "09"
        end_day = "30"
    end_date = year + month + end_day
    hd = pro.top10_floatholders(ts_code=f'{ts_code}', start_date=end_date, end_date=end_date)
    return hd


if __name__ == '__main__':
    # df = get_data()
    # write_data(df, 'stock_basic')
    hd = get_shareholder('600000.SH')
    write_data(hd, 'share_holder')
    con.close()

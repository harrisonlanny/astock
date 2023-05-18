from pandas import DataFrame, concat
from decimal import Decimal, getcontext
from datetime import date, timedelta, datetime
from ts.index import pro_bar, pro_api, fetch_daily
from db.index import show_tables, delete_table, create_table, insert_table, read_table, describe_table, copy_table
from model.index import get_columns
from model.model import TableModel
from utils.index import _map, parse_dataframe, print_dataframe
from utils.stock import fq
from service.index import api_query

# '600276.SH', '600276', '恒瑞医药', '江苏', '化学制药', '主板', 'L', datetime.date(2000, 10, 18)
# '002475.SZ', '002475', '立讯精密', '深圳', '元器件', '主板', 'L', datetime.date(2010, 9, 15)
# '688126.SH', '688126', '沪硅产业', '上海', '半导体', '科创板', 'L', datetime.date(2020, 4, 20)
# '600612.SH', '600612', '老凤祥', '上海', '服饰', '主板', 'L', datetime.date(1992, 8, 14)

# 两个任务：1. 比较未复权数据准确性 2. 如果未复权数据准确，则根据我们的算法算出前复权数据，再进行比较准确性

# ts_codes = ['688072.SH', '601288.SH', '600276.SH', '002475.SZ']
# start_date = '20200101'
# end_date = '20230517'
#
# test_dates = ['20200303', '20210407', '20220909', '20230517']
#
#
df = fetch_daily(ts_code='600612.SH')
print(df)


# result = read_table('stock_basic', filter_str="WHERE `name` like '%老凤祥%'")
# print(result)

# demo_df = DataFrame(columns=['name', 'age'], data=[['孙正', 29], ['翟彬', 31]])
# demo_df2 = DataFrame(columns=['name', 'age'], data=[['橘子', 5], ['橘子皮', 4]])
# demo_df = demo_df.sort_index(ascending=False)
#
# print(concat([demo_df, demo_df2], ignore_index=True))

# iloc是按照index来
# loc按照index的名字来
# iloc就是我们心里认为的根据index来
# print(demo_df)
# print(demo_df.iloc[0])
# print(demo_df.loc[0])

# print(demo_df.iloc(1)['name'])
# print(demo_df.loc(1)['name'])

# d = datetime.strptime('20210101', '%Y%m%d').date()
# prev_d = d - timedelta(days=1)
#
# print(prev_d)
# prev_d = date.strftime(datetime.strptime("20200101", '%Y%m%d').date() - timedelta(days=1),'%Y%m%d')
# print(prev_d)

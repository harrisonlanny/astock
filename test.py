from pandas import DataFrame, concat
from decimal import Decimal, getcontext
from datetime import date, timedelta, datetime
from ts.index import pro_bar, pro_api, fetch_daily
from db.index import show_tables, delete_table, create_table, insert_table, read_table, describe_table, copy_table
from model.index import describe_json
from model.model import TableModel
from utils.index import _map, parse_dataframe, print_dataframe
from utils.stock import fq, _filter
from service.index import api_query, get_current_d_tables

# '600276.SH', '600276', '恒瑞医药', '江苏', '化学制药', '主板', 'L', datetime.date(2000, 10, 18)
# '002475.SZ', '002475', '立讯精密', '深圳', '元器件', '主板', 'L', datetime.date(2010, 9, 15)
# '688126.SH', '688126', '沪硅产业', '上海', '半导体', '科创板', 'L', datetime.date(2020, 4, 20)
# '600612.SH', '600612', '老凤祥', '上海', '服饰', '主板', 'L', datetime.date(1992, 8, 14)
# '600000.SH', '600000', '浦发银行', '上海', '银行', '主板', 'L', datetime.date(1999, 11, 10), None, 'H'

# 两个任务：1. 比较未复权数据准确性 2. 如果未复权数据准确，则根据我们的算法算出前复权数据，再进行比较准确性

# ts_codes = ['688072.SH', '601288.SH', '600276.SH', '002475.SZ']
# start_date = '20200101'
# end_date = '20230517'
#
# test_dates = ['20200303', '20210407', '20220909', '20230517']
# #
# #
ts_code = '600276.SH'
symbol = ts_code.split('.')[0]


def optimize_result(result):

    return result


result = read_table('d_600276')
result = optimize_result(result)
# 经过修正，输出新的close_qfq
test_column_names = ['date', 'close_qfq']
test = [('20060403', '-0.27'), ('20110913', '4.53'), ('20160126', '12.51'), ('20190704', '45.42'),
        ('20220628', '35.50'), ('20220701', '38.78')]
test_times = _map(test, lambda item: item[0])
test_df = DataFrame(columns=test_column_names, data=test)

print(test_df)

jack = _filter(result, lambda row: date.strftime(row[1], '%Y%m%d') in test_times)
jack = _map(jack, lambda item: (date.strftime(item[1], '%Y%m%d'), item[-1]))
jack_df = DataFrame(columns=test_column_names, data=jack)
print(jack_df)

r = []
for index, item in enumerate(jack):
    test_item = test[index]
    gap = Decimal(str(item[1])) - Decimal(str(test_item[1]))
    r.append((item[0], str(gap)))

print(r)

# [('20060403', '0.991593'), ('20110913', '0.68664'), ('20160126', '0.5258'), ('20190704', '0.0952'), ('20220628', '0.0214'), ('20220701', '0.0087')]


# d_tables = get_current_d_tables()


# errors = _filter(d_tables, lambda item: 'PRIMARY' in item)
# print(d_tables)
# insert_table(f"d_{symbol}", column_names=desc.safe_column_names, row_list=values)

# print(fq(df).query(f'trade_date in {test_dates}'))


# result = read_table('stock_basic', filter_str="WHERE `name` like '%浦发银行%'")
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

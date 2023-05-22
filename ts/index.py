import tushare as ts
from pandas import DataFrame, concat
from datetime import datetime, date, timedelta

from constants import PAGE_SIZE
from utils.index import dict_kv_convert, _map, add_date_str, replace_nan_from_dataframe, _is_empty

ts.set_token('f3d6e975a07792eeb2978aed05fc7c4b31d408287d74ab0daf6a4464')

pro_api = ts.pro_api()
pro_bar = ts.pro_bar

# bs_ts_map = {
#     "date": "trade_date",
#     "code": "ts_code",
#     "preclose": "pre_close",
#     "volume": "vol",
#     "adjustflag": "adj_factor",
#     "pctChg": "pct_chg"
# }
# ts_bs_map = dict_kv_convert(bs_ts_map)

fields_map_df = DataFrame(columns=['ts', 'bs'], data=[
    ['trade_date', 'date'],
    ['ts_code', 'code'],
    ['pre_close', 'preclose'],
    ['vol', 'volume'],
    ['adj_factor', 'adjustflag'],
    ['pct_chg', 'pctChg'],
    ['open', 'open'],
    ['close', 'close'],
    ['high', 'high'],
    ['low', 'low'],
    ['amount', 'amount']
])


def format_fields(fields: list[str], _from: str, _to: str):
    result = []
    for field in fields:
        query_result = fields_map_df.query(f"{_from}=='{field}'")
        if len(query_result) > 0:
            result.append(query_result[_to].iloc[0])
    return result


# 该方法两大bug：
# 1. start_date写死成了''
# 2. end_date -1 太鲁莽（如果是单只股票，逻辑没问题，但多只的时候，end_date当天的数据未必拿完了）
def fetch_daily(ts_code: str = '', start_date: str = '', end_date: str = '', **kwargs):
    result: DataFrame = None
    start_date = start_date
    end_date = end_date
    should_query = True
    while should_query:
        print('query daily', f"from {start_date} to {end_date}")
        df = pro_api.query('daily', ts_code=ts_code, start_date=start_date, end_date=end_date, **kwargs)
        df = df.sort_index(ascending=False)
        # df = replace_nan_from_dataframe(df)

        # tushare返回的df是从上到下 最近的，到最古老的数据
        # 那么如果数据达到PAGE_SIZE，但日期还没到，则遗漏的数据 都一定是更古老的数据
        # 那么按逻辑start_date填空字符串表示我们想要无限古老的数据
        # end_date则从df里拿第一条数据（我们逆序了，所以最古老的排第一），取其中的trade_date - 1 作为end_date

        if result is None:
            result = df
        else:
            result = concat([df, result], ignore_index=True)

        print(len(df))
        should_query = len(df) == PAGE_SIZE
        if should_query:
            # start_date = ''
            # 20210101 => 20201231
            # end_date = date.strftime(datetime.strptime(df.iloc[0]['trade_date'], '%Y%m%d').date() - timedelta(days=1),
            #                          '%Y%m%d')
            end_date = df.iloc[0]['trade_date']
            # 请求end_date的数据，因为目前一共5000多股票，所以单独请求一天，哪怕是所有股票，数据也能一次请求完
            # 但这里有个隐患，就是一旦股票数超过6000，那么我们的方法就会出问题，
            # 但此时我们暂不做考虑，因为涨到6000家股票还有一段时间
            # 另外tushare官方也需要考虑此问题（如果不能一次性请求完，现阶段的api需要结合stock_basic才能知道哪些股票没有请求完，但这个逻辑很复杂，而且还涉及停牌股，所以需要tushare官方去修改对应api）
            # end_date = add_date_str(end_date, add_days=-1)
            if _is_empty(ts_code):
                # 1. 请求end_date日的数据（一次性能全部请求完，现阶段)
                end_date_df = fetch_daily(ts_code, start_date=end_date, end_date=end_date, **kwargs)
                # 2. 将当前df中所有end_date的数据删除
                result = result[result['trade_date'] != end_date]
                # 3. 将1的数据加到df头 concat([end_date_result, result], ignore_index=True)
                result = concat([end_date_df, result], ignore_index=True)
            # 4. end_date - 1，继续while循环
            end_date = add_date_str(end_date, add_days=-1)
            print('end_date', end_date)

    return result

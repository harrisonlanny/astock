import tushare as ts
from pandas import DataFrame,concat
from datetime import datetime, date, timedelta

from constants import PAGE_SIZE

ts.set_token('f3d6e975a07792eeb2978aed05fc7c4b31d408287d74ab0daf6a4464')

pro_api = ts.pro_api()
pro_bar = ts.pro_bar


def fetch_daily(ts_code: str, start_date: str = '', end_date: str = '', **kwargs):
    result: DataFrame = None
    start_date = start_date
    end_date = end_date
    should_query = True
    while should_query:
        print('query daily', f"from {start_date} to {end_date}")
        df = pro_api.query('daily', ts_code=ts_code, start_date=start_date, end_date=end_date, **kwargs)
        df = df.sort_index(ascending=False)

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
            start_date = ''
            # 20210101 => 20201231
            end_date = date.strftime(datetime.strptime(df.iloc[0]['trade_date'], '%Y%m%d').date() - timedelta(days=1),
                                     '%Y%m%d')
            print('end_date', end_date)

    return result

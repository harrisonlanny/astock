import numpy
from pandas import DataFrame
from utils.index import parse_dataframe, _filter, str2date, add_date, _map


def fq(df: DataFrame):
    # bunch_decimal(df, ['close', 'pre_close', 'open', 'high', 'low'])
    # getcontext().prec = 40
    df["pct_chg_fq"] = df['close'] / df['pre_close'] - 1
    # df["pct_chg_fq"] = df['pct_chg']
    df["adj_factor"] = (1 + df["pct_chg_fq"]).cumprod()
    latest = df.iloc[-1]
    df["close_qfq"] = df['adj_factor'] * (
            latest['close'] / latest['adj_factor']
    )
    close_scale = df['close_qfq'] / df['close']
    df["open_qfq"] = df['open'] * close_scale
    df["high_qfq"] = df['high'] * close_scale
    df['low_qfq'] = df['low'] * close_scale

    df.pop('pct_chg_fq')
    column_names = _filter(df.columns.values, lambda cname: cname != 'close_qfq')
    column_names.append('close_qfq')
    df = df[column_names]
    return df


def add_adj_factor(df: DataFrame, init_adj_factor=1):
    df["pct_chg_fq"] = df['close'] / df['pre_close'] - 1
    df["adj_factor"] = (1 + df["pct_chg_fq"]).cumprod() * init_adj_factor
    df.pop("pct_chg_fq")
    return df


# 日线转周线
# (默认日期为trade_date, 格式%Y-%m-%d)
# week命名：该周的最后一个交易日（周五）的日期
def d_to_w(data: list[dict], date_format: str = "%Y%m%d", last_trade_weekday: int = 5):
    if data is None:
        return data
    w_map = {}
    for d in data:
        # 1. 获取日期对应星期几
        _date = str2date(d['trade_date'], date_format)
        weekday = _date.isoweekday()
        # 2. 用 5-w 得到距离周五还有n天
        gap = last_trade_weekday - weekday
        # 3. 得到周5的日期，作为w_map的key
        weekday_str = add_date(_date, add_days=gap, str_format=date_format, result_type='str')

        if w_map.get(weekday_str) is None:
            w_map[weekday_str] = []
        w_map[weekday_str].append(d)

    result = []
    for d_str in w_map:
        _list = w_map[d_str]
        open = _list[0]["open"]
        close = _list[-1]["close"]
        high_list = _map(_list, lambda item: item["high"])
        low_list = _map(_list, lambda item: item["low"])
        amount_list = _map(_list, lambda item: item["amount"])
        vol_list = _map(_list, lambda item: item["vol"])

        high = numpy.max(high_list)
        low = numpy.min(low_list)
        amount = numpy.sum(amount_list)
        vol = numpy.sum(vol_list)

        result.append({
            "trade_date": d_str,
            "open": open,
            "close": close,
            "high": high,
            "low": low,
            "amount": amount,
            "vol": vol
        })
    return result

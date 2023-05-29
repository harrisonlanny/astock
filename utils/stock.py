from datetime import date

import numpy
from pandas import DataFrame
from utils.index import parse_dataframe, _filter, str2date, add_date, _map, date2str, _is_empty


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


def d_to_adj(data: list[dict], adj_type: str = None):
    if _is_empty(adj_type) or _is_empty(data):
        return data
    target = data[-1] if adj_type == 'q' else data[0]
    unit = target['close'] / target['adj_factor']
    result: list[dict] = []
    for d in data:
        new_d = d.copy()
        adj = new_d['adj_factor']
        old_close = new_d['close']
        new_close = adj * unit
        scale = new_close / old_close

        new_d['close'] = new_close
        new_d['open'] = new_d['open'] * scale
        new_d['high'] = new_d['high'] * scale
        new_d['low'] = new_d['low'] * scale

        result.append(new_d)
    return result


def d_to_n(data: list[dict], date_format: str = "%Y%m%d", get_key=None):
    if data is None:
        return data
    w_map = {}
    for d in data:
        # 1. 获取日期对应月份 作为w_map的key
        _date = str2date(d['trade_date'], date_format)
        key = get_key(_date)

        if w_map.get(key) is None:
            w_map[key] = []
        w_map[key].append(d)

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


# 日线转周线
# (默认日期为trade_date, 格式%Y-%m-%d)
# week命名：该周的最后一个交易日（周五）的日期
def d_to_w(data: list[dict], date_format: str = "%Y%m%d", key_format: str = "%Y%m%d", last_trade_weekday: int = 5):
    return d_to_n(data, date_format,
                  get_key=lambda d: add_date(d, add_days=last_trade_weekday - d.isoweekday(), str_format=key_format,
                                             result_type='str'))


def d_to_m(data: list[dict], date_format: str = "%Y%m%d", key_format: str = "%Y%m"):
    return d_to_n(data, date_format, get_key=lambda d: date2str(d, key_format))


def d_to_y(data: list[dict], date_format: str = "%Y%m%d", key_format: str = "%Y"):
    return d_to_n(data, date_format, get_key=lambda d: date2str(d, key_format))

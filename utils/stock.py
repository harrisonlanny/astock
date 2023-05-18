from pandas import DataFrame
from utils.index import parse_dataframe, _filter


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


def fq2(df: DataFrame):
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

from pandas import DataFrame


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
    # df['close_qfq'] = round(df['close_qfq'], 2)
    return df

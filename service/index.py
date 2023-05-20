import time

from pandas import DataFrame

from db.index import delete_table, create_table, insert_table, read_table, show_tables, insert_update_table, sql, \
    get_last_row, clear_table
from ts.index import pro_api, fetch_daily
from utils.index import parse_dataframe, _filter, get_diff, _map, get_diff2, _set, _find, _find_index, add_date
import datetime
from model.index import describe_json
from constants import DATE_FORMAT
from utils.stock import add_adj_factor


def api_query(api_name, fields=None, fields_name=None, **kwargs):
    if fields_name is None:
        fields_name = api_name
    if fields is None:
        describe = describe_json(fields_name)
        fields = ','.join(describe.column_names)
    print('query fields:', fields)
    df = pro_api.query(api_name, fields, **kwargs)
    return parse_dataframe(df)


def refresh_table(table_name, df=None):
    # 获取model 表结构
    describe = describe_json(table_name)
    fields = ','.join(describe.column_names)
    # 从tushare获取数据
    if df is None:
        df = pro_api.query(table_name, fields=fields)
    # 将dataframe转成通用格式
    _columns, data = parse_dataframe(df)
    delete_table(table_name)
    create_table(table_name, describe.safe_columns)
    insert_table(table_name, describe.safe_column_names, data)


def format_table_value(value):
    if isinstance(value, datetime.date):
        return value.strftime(DATE_FORMAT)
    return value


def format_table_row(row: list | tuple):
    return _map(row, lambda item: format_table_value(item))


def refresh_table_stock_basic(fast: bool = True):
    table_name = 'stock_basic'
    live_stocks = api_query(table_name, list_status='L')[1]
    dead_stocks = api_query(table_name, list_status='D')[1]
    pause_stocks = api_query(table_name, list_status='P')[1]

    new_stocks = live_stocks + dead_stocks + pause_stocks
    print('new_stocks', len(new_stocks), '\n')
    old_stocks = read_table(table_name)
    print('old_stocks', len(old_stocks), '\n')
    # column_name_list = get_columns_info(table_name)[1]
    describe = describe_json(table_name)
    if fast:
        print('fast model to refresh stock_basic', 'only check ts_code & list_status')
        # column_name_list = get_columns_info(table_name)[1]
        # id = "{ts_code}__{list_status}"
        ts_code_index = _find_index(describe.column_names, lambda cname: cname == 'ts_code')
        list_status_index = _find_index(describe.column_names, lambda cname: cname == 'list_status')
        generate_id = lambda row: f"{row[ts_code_index]}__{row[list_status_index]}"
        new_ids = _map(new_stocks, generate_id)
        old_ids = _map(old_stocks, generate_id)

        diff_ids = get_diff(new_ids, old_ids)
        diff_ts_codes = _map(diff_ids, lambda id: id.split('__')[0])
        print('diff_ts_codes', diff_ts_codes)
        need_insert_stocks = _map(diff_ts_codes, lambda ts_code: _find(new_stocks, lambda item: item[0] == ts_code))
    else:
        old_stocks = _map(read_table(table_name), lambda row: format_table_row(row))
        need_insert_stocks = get_diff2(new_stocks, old_stocks, lambda v1, v2: _set(v1) == _set(v2))

    print('need_insert_stocks: ', len(need_insert_stocks), need_insert_stocks)

    # 调用db方法，插入或更新数据
    insert_update_table(table_name, describe.column_names, need_insert_stocks)


def get_current_d_tables():
    return _filter(show_tables(), lambda item: item.startswith('d_'))


def get_new_symbols():
    # 现在已经存在的日线表
    d_tables = get_current_d_tables()
    # print('d_tables',d_tables)
    # stock_basic表里所有stock
    symbols = _map(read_table('stock_basic', ['symbol']), lambda item: f"d_{item[0]}")
    # print('stock_basic symbols', symbols)
    new_symbols = get_diff(symbols, d_tables)

    return _map(new_symbols, lambda symbol: symbol[2:])


def get_ts_code_from_symbol(symbol):
    result = read_table("stock_basic", ["ts_code"], filter_str=f"WHERE `symbol`='{symbol}'")
    if len(result):
        return result[0][0]
    return None


def create_new_d_tables():
    # safe_columns, column_names, column_names_str, safe_column_names_str = get_columns_info('d')
    describe = describe_json('d')
    new_symbols = get_new_symbols()
    for new_symbol in new_symbols:
        d_table_name = f"d_{new_symbol}"
        create_table(d_table_name, describe.safe_columns)
        print('create_table', d_table_name)


def delete_d_tables():
    d_tables = get_current_d_tables()
    for d_name in d_tables:
        delete_table(d_name)


# 如果表为空，返回None
# PS: harrison写给lanny：受你启发，写出了db.index.get_latest_row()
# def get_latest_row_from_d_table(table_name: str):
#     result = read_table(table_name, ['trade_date'], "ORDER BY trade_date DESC LIMIT 1")
#     if len(result):
#         return result[0]
#     return None


def update_d_tables():
    # 1. 遍历所有的d_tables
    d_tables = get_current_d_tables()
    # d_tables = ["d_000001", "d_000002"]
    # 2. 调用get_latest_trade_date_from_d_table(table_name)
    desc = describe_json("d")
    date_code_map = {}
    for table_index, d_table in enumerate(d_tables):
        last_row = get_last_row(d_table, fields=['trade_date'], order_by='trade_date')
        symbol = d_table[2:]
        ts_code = get_ts_code_from_symbol(symbol)
        # 3. 如果返回的为空，调用fetch_daily(ts_code)填充该表
        if last_row is None:
            print(f"{d_table}表为空，调用fetch_daily({ts_code})", f"{table_index + 1}/{len(d_tables)}")
            daily_df = fetch_daily(ts_code)
            time.sleep(1.5)
            daily_df = add_adj_factor(daily_df)
            # 从djson中拿到真正会被用到的字段（即非open_qfq、close_qfq...）
            sort_fields = _filter(desc.column_names, lambda field: not field.endswith('_qfq'))
            print('api返回字段排序后：', sort_fields)
            daily_df = daily_df[sort_fields]
            fields, values = parse_dataframe(daily_df)
            clear_table(d_table)
            insert_table(d_table, sort_fields, values)
            print('insert_table完成!')
        # 4. 得到[('ts_code', 'latest_trade_date'),...]后，将latest_trade_date相同的ts_code分类
        else:
            last_trade_date = last_row['trade_date']
            if date_code_map.get(last_trade_date) is None:
                date_code_map[last_trade_date] = []
            date_code_map[last_trade_date].append(ts_code)

    print('date_code_map', date_code_map)
    # 5. 得到 如: {'latest_trade_date1': [ts_code1, ts_code2],'latest_trade_date2': [ts_code3, ts_code4]...}
    for last_trade_date in date_code_map:
        ts_codes = date_code_map[last_trade_date]
        start_date_str = add_date(last_trade_date, add_days=1, result_type='str')
        if len(ts_codes) > len(d_tables) / 2:
            df = fetch_daily(start_date=start_date_str)
            df = df[df['ts_code'].isin(ts_codes)]
            fields, values = parse_dataframe(df)
            # [
            # ['000001.SZ', '20230101', 1],
            # ['000002.SZ', '20230101', 1],
            # ['000001.SZ', '20230102', 1],
            # ['000002.SZ', '20230102', 1],
            # ]

            # {
            #   '000001.SZ': [['000001.SZ', '20230101', 1], ['000001.SZ', '20230101', 1]]
            #   '000002.SZ': [['000002.SZ', '20230101', 1],['000002.SZ', '20230101', 1]]
            #  }
            code_values_map = {}
            for value in values:
                ts_code = value[0]
                if code_values_map[ts_code] is None:
                    code_values_map[ts_code] = []
                code_values_map[ts_code].append(value)

            for ts_code in code_values_map:
                table_name = f"d_{ts_code.split('.')[0]}"
                values = code_values_map[ts_code]
                _df = DataFrame(columns=fields, data=values)
                # 从数据库取最后一条数据，取出它的adj_factor做初始值
                last_row = get_last_row(table_name, fields=['adj_factor'], order_by='trade_date')
                init_adj_factor = last_row['adj_factor']
                # 为新增加的数据计算出adj_factor
                _df = add_adj_factor(_df, init_adj_factor=init_adj_factor)
                _fields, _values = parse_dataframe(_df)
                print(f"大众，{table_name}表插入数据：", _fields, '\n', _values)
                insert_table(table_name, _fields, _values)
        else:
            # [
            # ['000001.SZ', '20230101', 1],
            # ['000001.SZ', '20230102', 1],
            # ]
            for ts_code in ts_codes:
                df = fetch_daily(ts_code, start_date=start_date_str)
                time.sleep(1.5)
                fields, values = parse_dataframe(df)
                table_name = f"d_{ts_code.split('.')[0]}"
                # 从数据库取最后一条数据，取出它的adj_factor做初始值
                last_row = get_last_row(table_name, fields=['adj_factor'], order_by='trade_date')
                init_adj_factor = last_row['adj_factor']
                # 为新增加的数据计算出adj_factor
                df = add_adj_factor(df, init_adj_factor=init_adj_factor)
                _fields, _values = parse_dataframe(df)
                print(f"小众，{table_name}表插入数据：", _fields, '\n', _values)
                insert_table(table_name, _fields, _values)


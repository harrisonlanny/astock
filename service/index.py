from db.index import delete_table, create_table, insert_table, read_table, show_tables, insert_update_table, sql
from ts.index import pro_api
from utils.index import parse_dataframe, _filter, get_diff, _map, get_diff2, _set, _find, _find_index
import datetime
from model.index import describe_json
from constants import DATE_FORMAT


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
# PS: harrison写给lanny：受你启发，写出了db.index.get_latest_row()，因为已经足够方便，故不需要此service方法了
# def get_latest_row_from_d_table(table_name: str):
#     result = read_table(table_name, ['trade_date'], "ORDER BY trade_date DESC LIMIT 1")
#     if len(result):
#         return result[0]
#     return None


def update_d_tables():
    # 0. 调用read_table('stock_basic')，方便接下来根据table_name拿到ts_code
    # 1. 遍历所有的d_tables
    # 2. 调用get_latest_trade_date_from_d_table(table_name)
    # 3. 如果返回的为空，调用fetch_daily(ts_code)填充该表
    # 4. 得到[('ts_code', 'latest_trade_date'),...]后，将latest_trade_date相同的ts_code分类
    # 5. 得到 如: [('latest_trade_date1', ts_codes),('latest_trade_date2', ts_codes)...]
    # 6. 根据5的分类，如果ts_codes为单条，fetch_daily(ts_code, start_date, end_date)
    #               如果ts_codes为多条，因为根据逻辑，大概率是几千条，所以直接fetch_daily(start_date, end_date)
    return None

# def update_d_table(table_name):
#
#     # 1. 判断是全量更新还是增量更新（如果除权，需要将所有日线数据都更新的，我们只获取前除权日线）
#     #      方法：取该股票上市第一天的开盘价 （从数据库和tushare两个源），然后对比这两个源拿到的数据是否一致，
#     #      如果一致，认为没有除权，则增量更新即可；反之 全量更新
#     first_day_open_from_table = read_table(table_name, )
#     first_day_open_from_api =
#     should_full_update = first_day_open_from_table != first_day_open_from_api
#
#     # 2. 如果是全量更新，就将能请求到的所有日线数据都insert
#     if should_full_update:
#         all_values =
#         insert_table(table_name, )
#     # 3. 如果是增量更新，就拿表中最后一条数据的日期到最新的日期的数据
#     else:
#         latest_date = read_table()
#         api(latest_date, now)
#         insert_table(table_name)

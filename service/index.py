from db.index import delete_table, create_table, insert_table, read_table, show_tables
from model.index import get_columns_info
from ts.index import pro_api
from utils.index import parse_dataframe, _filter, get_diff, _map, get_diff2, _set, _find
import datetime


def api_query(api_name, fields=None, fields_name=None, **kwargs):
    if fields_name is None:
        fields_name = api_name
    if fields is None:
        safe_columns, column_names, names_str, safe_name_str = get_columns_info(fields_name)
        fields = names_str
    print('query fields:', fields)
    df = pro_api.query(api_name, fields, **kwargs)
    return parse_dataframe(df)


def refresh_table(table_name, df=None):
    # 获取model 表结构
    safe_columns, column_names, names_str, safe_name_str = get_columns_info(table_name)
    # 从tushare获取数据
    if df is None:
        df = pro_api.query(table_name, fields=names_str)
    # 将dataframe转成通用格式
    _columns, data = parse_dataframe(df)
    delete_table(table_name)
    create_table(table_name, safe_columns)
    insert_table(table_name, column_names, data)


def format_table_value(value):
    if isinstance(value, datetime.date):
        return value.strftime('%Y%m%d')
    return value


def format_table_row(row: list | tuple):
    return _map(row, lambda item: format_table_value(item))


def refresh_table_stock_basic():
    table_name = 'stock_basic'
    live_stocks = api_query(table_name, list_status='L')[1]
    dead_stocks = api_query(table_name, list_status='D')[1]
    pause_stocks = api_query(table_name, list_status='P')[1]

    new_stocks = live_stocks + dead_stocks + pause_stocks
    print('new_stocks', len(new_stocks), '\n')
    # old_stocks = _map(read_table(table_name), lambda row: format_table_row(row))
    old_stocks = read_table(table_name)
    print('old_stocks', len(old_stocks), '\n')

    # id = "{ts_code}__{list_status}"
    generate_id = lambda row: f"{row[0]}__{row[6]}"
    new_ids = _map(new_stocks, generate_id)
    old_ids = _map(old_stocks, generate_id)

    diff_ids = get_diff(new_ids, old_ids)
    diff_ts_codes = _map(diff_ids, lambda id:id.split('__')[0])
    print('diff_ts_codes', diff_ts_codes)

    need_insert_stocks = _map(diff_ts_codes, lambda ts_code: _find(new_stocks, lambda item: item[0] == ts_code))
    print('need_insert_stocks', need_insert_stocks)






    # need_insert_stocks = get_diff2(new_stocks, old_stocks, lambda v1, v2: _set(v1) == _set(v2))
    # print('need_insert_stocks: ', len(need_insert_stocks), need_insert_stocks)
    #
    # target = _find(need_insert_stocks, lambda item: item[0] == '838837.BJ')
    # print('target', target)


def get_current_d_tables():
    return _filter(show_tables(), lambda item: item.startswith('d_'))


def get_new_symbols():
    # 现在已经存在的日线表
    d_tables = get_current_d_tables()
    # print(d_tables)
    # stock_basic表里所有stock
    symbols = _map(read_table('stock_basic', ['symbol']), lambda item: f"d_{item[0]}")
    # print(symbols)
    new_symbols = get_diff(symbols, d_tables)

    return new_symbols


def create_new_d_tables():
    safe_columns, column_names, column_names_str, safe_column_names_str = get_columns_info('d')
    new_symbols = get_new_symbols()
    for new_symbol in new_symbols:
        d_table_name = f"d_{new_symbol}"
        create_table(d_table_name, safe_columns)
        print('create_table', d_table_name)

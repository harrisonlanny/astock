from db.index import delete_table, create_table, insert_table, read_table, show_tables
from model.index import get_columns_info
from ts.index import pro_api
from utils.index import parse_dataframe, _filter, get_diff, _map


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


def get_new_symbols():
    # 现在已经存在的日线表
    d_tables = _filter(show_tables(), lambda item: item.startswith('d_'))
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


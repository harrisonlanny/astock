import json
from utils.index import get_path, _map

tables_json_path = get_path('model/tables.json')
print('tables_json_path: ', tables_json_path)


def get_columns(table_name):
    with open(tables_json_path, 'r') as f:
        tables = json.load(f)
        columns = tables[table_name]['columns']
        return columns


def add_fyh_for_column_name(column_str: str):
    arr = column_str.split(' ')
    arr[0] = f"`{arr[0]}`"
    result = " ".join(arr)
    return result


def get_safe_columns(columns):
    return _map(columns, lambda column_str: add_fyh_for_column_name(column_str))


def get_column_name_list(columns):
    column_name_list = _map(columns, lambda item: item.split()[0])
    return column_name_list


def get_columns_info(table_name):
    columns = get_columns(table_name)
    safe_columns = get_safe_columns(columns)
    column_name_list = get_column_name_list(columns)
    columns_name_str = ','.join(column_name_list)
    safe_columns_name_str = ','.join(_map(column_name_list, lambda column_name: f"`{column_name}`"))
    return safe_columns, column_name_list, columns_name_str, safe_columns_name_str


if __name__ == '__main__':
    columns = get_columns('stock_basic')
    print('columns:', columns)
    columns_name_list = get_column_name_list(columns)
    print('columns_name_list', columns_name_list)

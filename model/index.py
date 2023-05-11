import json
from utils.index import get_path,_map

tables_json_path = get_path('model/tables.json')
print('tables_json_path: ', tables_json_path)


def get_columns(table_name):
    with open(tables_json_path, 'r') as f:
        tables = json.load(f)
        columns = tables[table_name]['columns']
        return columns


def get_column_name_list(columns):
    column_name_list = _map(columns, lambda item: item.split()[0])
    return column_name_list


if __name__ == '__main__':
    columns = get_columns('stock_basic')
    print('columns:', columns)
    columns_name_list = get_column_name_list(columns)
    print('columns_name_list', columns_name_list)

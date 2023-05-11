import json
from utils.index import get_path

tables_json_path = get_path('model/tables.json')
print('tables_json_path: ', tables_json_path)


def get_columns(table_name):
    with open(tables_json_path, 'r') as f:
        tables = json.load(f)
        columns = tables[table_name]['columns']
        return columns


def get_column_name_list(columns):
    column_name_list = list(map(lambda col: col.split()[0], columns))
    return column_name_list

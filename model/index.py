import json

from model.model import TableModel
from utils.index import get_path, _map

tables_json_path = get_path('model/tables.json')
print('tables_json_path: ', tables_json_path)


def get_columns(table_name):
    with open(tables_json_path, 'r') as f:
        tables = json.load(f)
        columns = tables[table_name]['columns']
        return columns


def describe_json(table_name):
    return TableModel(columns=get_columns(table_name))

#
# if __name__ == '__main__':

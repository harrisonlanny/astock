import re
from utils.index import _filter, _find_index, _find, _map


class TableModel:
    table_name: str
    primary_key: str
    engine: str
    charset: str
    describe: str
    columns: list[str]
    column_names: list[str]
    safe_columns: list[str]
    safe_column_names: list[str]

    def __init__(self, describe: str = None, columns: list[str] = None):
        if describe:
            self.describe = describe
            parsed_result = TableModel.parse_describe(describe)
            self.table_name = parsed_result['table_name']
            self.primary_key = parsed_result['primary_key']
            self.engine = parsed_result['engine']
            self.charset = parsed_result['charset']
            self.columns = parsed_result['columns']
            self.column_names = TableModel.get_column_names(self.columns)
            self.safe_columns = TableModel.get_safe_columns(self.columns)
            self.safe_column_names = TableModel.get_safe_column_names(self.column_names)
        elif columns is not None:
            self.columns = columns
            self.column_names = TableModel.get_column_names(self.columns)
            self.safe_columns = TableModel.get_safe_columns(self.columns)
            self.safe_column_names = TableModel.get_safe_column_names(self.column_names)
            pk_index = _find_index(self.columns, lambda item: "PRIMARY KEY" in item)
            if pk_index:
                self.primary_key = self.column_names[pk_index]

        # 目标是：入参columns (参考tables.json) 出参：columns、column_names 、safe_columns、safe_column_names

    @staticmethod
    def parse_describe(describe: str):
        result = {
            "table_name": None,
            "primary_key": None,
            "engine": None,
            "charset": None,
            "columns": []
        }
        describe = describe.replace("\n", "")
        match = re.match(r'^CREATE TABLE `(\w+)` \((.+)\) (.*)', describe)
        # print('match', match.groups())
        groups = match.groups()

        result['table_name'] = groups[0].strip()
        parsed_columns = groups[1].strip()
        parsed_suffix = groups[2].strip()

        suffix_arr = parsed_suffix.split(" ")
        engine = _find(suffix_arr, lambda item: 'ENGINE' in item)
        if engine:
            result['engine'] = engine.split("=")[1]

        charset = _find(suffix_arr, lambda item: 'CHARSET' in item)
        if charset:
            result['charset'] = charset.split("=")[1]

        parsed_columns = _map(parsed_columns.split(','), lambda item: item.strip().replace("`", ""))

        primary_key_str = _find(parsed_columns, lambda item: "PRIMARY KEY" in item)
        if primary_key_str:
            parsed_columns = _filter(parsed_columns, lambda item: item != primary_key_str)
            result['primary_key'] = primary_key_str.split('PRIMARY KEY ')[1][1:-1].strip()
            primary_key_index = _find_index(parsed_columns, lambda item: result['primary_key'] in item)
            parsed_columns[primary_key_index] = f"{parsed_columns[primary_key_index]} PRIMARY KEY"

        result['columns'] = parsed_columns

        return result

    @staticmethod
    def add_fyh_for_column_name(column: str):
        arr = column.split(' ')
        arr[0] = f"`{arr[0]}`"
        result = " ".join(arr)
        return result

    @staticmethod
    def get_safe_columns(columns):
        return _map(columns, lambda column_str: TableModel.add_fyh_for_column_name(column_str))

    @staticmethod
    def get_column_names(columns):
        return _map(columns, lambda item: item.split()[0])

    @staticmethod
    def get_safe_column_names(column_names):
        return _map(column_names, lambda item: f"`{item}`")

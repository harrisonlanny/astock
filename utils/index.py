import os

import pandas

from collections.abc import Iterable

from constants import PROJECT_NAME


def get_current_path():
    return os.path.abspath(os.path.dirname(__file__))


def get_root_path():
    cur_path = get_current_path()
    index = cur_path.find(PROJECT_NAME) + len(PROJECT_NAME)
    return cur_path[:index]


def get_path(src_relative_path):
    str_path = str(src_relative_path)
    _path = str_path if str_path.startswith('/') else '/' + str_path
    return f"{get_root_path()}{_path}"


def _map(data_list, callback):
    return list(map(callback, data_list))


def is_subset(list1, list2):
    return set(list2).issubset(set(list1))


def _safe_join(data_list, connect_symbol=','):
    return str(connect_symbol).join(_map(data_list, lambda item: str(item)))


def add_single_quotation(value):
    if isinstance(value, str):
        return f"'{value}'"
    return value


def add_double_quotation(value):
    if isinstance(value, str):
        return f'"{value}"'
    return value


def none_to_null_str(v):
    if v is None:
        return "null"
    return v


def parse_dataframe(df: pandas.DataFrame):
    columns = df.columns.values
    values = df.to_numpy().tolist()

    return columns, values


def is_iterable(value):
    return isinstance(value, Iterable)


if __name__ == '__main__':
    print(get_path('/model/tables.json'))

    print(",".join(_map([1, 2, 3], lambda item: str(item))))

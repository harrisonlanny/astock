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


def _filter(data_list, callback):
    return list(filter(callback, data_list))


def _find(data_list, callback):
    result = None
    for data in data_list:
        if callback(data):
            result = data
            break
    return result


def _find_index(data_list, callback):
    result = None
    for index, data in enumerate(data_list):
        if callback(data):
            result = index
            break
    return result


def is_subset(list1, list2):
    return set(list2).issubset(set(list1))


def get_diff(list1, list2):
    return list(set(list1) - set(list2))


# list1: [1,[2],3]
# list2: [4,[2],5,6]
# result: [1,3]
def get_diff2(list1, list2, is_same_callback=lambda v1, v2: v1 == v2):
    result = []
    for v1 in list1:
        is_in = _in(list2, v1, is_same_callback)
        if not is_in:
            result.append(v1)
    return result


def _in(data_list, value, is_same_callback=lambda v1, v2: v1 == v2):
    if is_same_callback is None:
        return value in data_list
    one = _find(data_list, lambda item: is_same_callback(item, value))
    return one is not None


def _set(v):
    if is_iterable(v):
        return set(v)
    return v


def get_intersection(list1, list2):
    return list(set(list1).intersection(set(list2)))


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

import os
from collections.abc import Iterable
from datetime import datetime, date, timedelta
from decimal import Decimal

import pandas
from pandas import DataFrame

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
    if data_list is None:
        return []
    return list(map(callback, data_list))


def _map2(data_list, callback):
    if data_list is None:
        return []
    result = []
    for index, item in enumerate(data_list):
        new_item = callback(item, index)
        result.append(new_item)
    return result


def _filter(data_list, callback):
    if data_list is None:
        return []
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


def print_dataframe(df: DataFrame):
    column_names, rows = parse_dataframe(df)
    print(column_names, '\n')
    for row in rows:
        kvs = {}
        for index, value in enumerate(row):
            kvs[column_names[index]] = value
        print(kvs, '\n')


def bunch_decimal(df: DataFrame, column_names: list[str]):
    for column_name in column_names:
        df[column_name] = df[column_name].apply(str)
        df[column_name] = df[column_name].apply(Decimal)
    return df


def dict_kv_convert(data: dict):
    result = {}
    for k in data:
        v = data[k]
        result[v] = k
    return result


def list2dict(keys: list, values: list):
    result = {}
    for index, key in enumerate(keys):
        value = values[index]
        result[key] = value
    return result


def add_date(d: date, add_days: int, result_type: str = 'date', str_format: str = '%Y%m%d'):
    result = d + timedelta(days=add_days)
    if result_type == 'date':
        return result
    return date.strftime(result, str_format)


def add_date_str(date_str: str, add_days: int, str_format: str = '%Y%m%d'):
    _date = datetime.strptime(date_str, str_format).date()
    new_date = add_date(_date, add_days)
    return date.strftime(new_date, str_format)


def str2date(date_str: str, str_format: str = '%Y%m%d'):
    return datetime.strptime(date_str, str_format).date()


def get_current_date():
    return datetime.now().date()


if __name__ == '__main__':
    print(get_path('/model/tables.json'))

    print(",".join(_map([1, 2, 3], lambda item: str(item))))

import os
import re
from collections.abc import Iterable
from datetime import datetime, date, timedelta
from decimal import Decimal
from json import dump, load
import statistics
import threading
import time

import numpy
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


def is_iterable(value):
    return isinstance(value, Iterable)


def _is_number(value):
    return isinstance(value, (int, float))


def _is_nan(value):
    if _is_number(value):
        return numpy.isnan(value)
    return False


def _is_empty(value):
    if value is None:
        return True
    if value == '':
        return True
    if _is_nan(value):
        return True
    if is_iterable(value) and len(value) == 0:
        return True
    return False


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

def _every(data_list, callback):
    result = True
    for data in data_list:
        if callback(data) == False:
            result = False
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


def replace_nan_from_dataframe(df: pandas.DataFrame, replace=None):
    print('check_nan', df.isnull())
    return df.astype(object).where(pandas.notnull(df), replace)


def parse_dataframe(df: pandas.DataFrame):
    columns = df.columns.values
    values = df.to_numpy().tolist()

    return columns, values


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


def get_dict_key_by_index(data: dict, index: int):
    # 5   -1 -2 -3 -4 -5 -6
    _list = list(data.keys())
    index_range = get_list_index_range(_list)
    if index > index_range[1] or index < index_range[0]:
        return None
    return _list[index]


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
    if isinstance(date_str, date):
        return date_str
    return datetime.strptime(date_str, str_format).date()


def date2str(d: date, _format: str = "%Y%m%d"):
    return date.strftime(d, _format)


def get_current_date():
    return datetime.now().date()


def avg(nums: list[float]):
    return sum(nums) / len(nums)


def has_chinese_number(data: str):
    chinese_number_list = [
        '一',
        '二',
        '三',
        '四',
        '五',
        '六',
        '七',
        '八',
        '九',
        '十'
    ]
    for chinese_number in chinese_number_list:
        if chinese_number in data:
            return True
    return False
def has_english_number(data):
    english_number_list = [0,1,2,3,4,5,6,7,8,9]
    for english_number in english_number_list:
        if str(english_number) in str(data):
            return True
    return False


def large_num_format(large_num: str):
    regular = r'^\d{1,3},(\d{3},)*(\d{3})(.\d+)?$|^[1-9]\d{1,2}$'
    if large_num is None:
        return None
    elif re.findall(regular, large_num):
        new_large_num = large_num.replace('，', '').replace(',', '')
        return float(new_large_num)
    return large_num

# 一、xxx 或 （一）xxx 或（一）、xxx
# consider_content: 是否考虑数字前缀后面的文本内容
# 如果为True -> 一、xxx这种ok；如果为False，则不ok
def is_chinese_number_prefix(text:str, consider_content:bool = True):
    regular_suffix = "" if consider_content else "$"
    # 顿号
    result = not _is_empty(re.findall(f'^[\\u4e00-\\u9fa5]+\\u3001{regular_suffix}', text))
    if result:
        return True
    # 括号（）
    result = not _is_empty(re.findall(f'^\\uff08[\\u4e00-\\u9fa5]+\\uff09\\u3001?{regular_suffix}', text))
    if result:
        return True
    
    return False

def is_alabo_number_prefix(text: str, consider_content: bool = True):
    regular_suffix = "" if consider_content else "$"
    # 顿号、
    result = not _is_empty(re.findall(f'^[0-9]+\\u3001{regular_suffix}', text))
    if result:
        return True
    
    # 中文括号（）
    result = not _is_empty(re.findall(f'^\\uff08[0-9]+\\uff09\\u3001?{regular_suffix}', text))
    if result:
        return True
    
    # 英文句号 . 包括(4).
    result = not _is_empty(re.findall(f'^[\w\W]+\\u002e{regular_suffix}', text))
    if result:
        return True
    
    # 英文括号 () (). ()、
    result = not _is_empty(re.findall(f'^\\u0028\w+\\u0029{regular_suffix}', text))
    if result:
        return True
    
    # 阿拉伯数字 6.47.2
    result = not _is_empty(re.findall(f'^\d.\d{regular_suffix}', text))
    if result:
        return True
    
    return False

def is_period_prefix(text:str):
    '''
    eg：2022年度合并利润表
    '''
    for item in text:
        if item in ["年","季","日","度"]:
            return True
    return False 

def mul_str(data: str, count: int):
    result = ''
    for index in range(0, count):
        result += data
    return result


def dict2str(data: dict, tab_symbol: str = '\t', tab_symbol_count: int = 1):
    result = '{\r\n'
    tab_symbols = mul_str(tab_symbol, tab_symbol_count)
    for key in data:
        value = data[key]
        value_str = format_str(value, tab_symbol, tab_symbol_count + 1)
        result += f"{tab_symbols}{key}: {value_str},\r\n"
    result += mul_str(tab_symbol, tab_symbol_count - 1) + "}"
    return result


def list2str(data: list | tuple, tab_symbol: str = '\t', tab_symbol_count: int = 1):
    result = '[\r\n'
    tab_symbols = mul_str(tab_symbol, tab_symbol_count)
    for value in data:
        value_str = format_str(value, tab_symbol, tab_symbol_count + 1)
        result += f"{tab_symbols}{value_str},\r\n"
    result += f"{mul_str(tab_symbol, tab_symbol_count - 1)}]"
    return result


def get_list_index_range(data: list | tuple):
    length = len(data)
    max_index = length - 1
    min_index = -length
    return min_index, max_index

def is_list_item_same(data: list|tuple):
    is_same = True
    for item in data:
        if item != data[0]:
            is_same = False
            break
    return is_same


def format_str(data: any, tab_symbol: str = '\t', tab_symbol_count: int = 1):
    if isinstance(data, str):
        return f"'{data}'"
    elif isinstance(data, dict):
        return dict2str(data, tab_symbol, tab_symbol_count)
    elif is_iterable(data):
        return list2str(data, tab_symbol, tab_symbol_count)
    return str(data)


def txt(output_path: str, chars: any):
    # 打开文本文件
    _output_path = get_path(output_path)
    file = open(_output_path, "w")
    # 写入数据到文件
    _str = format_str(chars)
    file.write(_str)
    # 关闭文件
    file.close()


def json(output_path: str, data: any = None):
    # 打开文本文件
    _output_path = get_path(output_path)
    result = None
    if data is None:
        f = open(_output_path, 'r')
        result = load(f)
    else:
        f = open(_output_path, 'w')
        dump(data, f, ensure_ascii=False)
    f.close()
    return result

# /User/Harrison/astock/xxx.json
# /xxx.json
def json2(output_path: str, data: any = None):
    if get_root_path() in output_path:
        _path = output_path
    else:
        _path = get_path(output_path)
    # print("_path: ", _path)
    result = None
    if data is None:
        f = open(_path, 'r')
        result = load(f)
    else:
        f = open(_path, 'w')
        dump(data, f, ensure_ascii=False)
    f.close()
    return result


def _dir(dir_path: str, file_types: list[str] = None):
    _path = get_path(dir_path)
    if file_types is None:
        file_types = []
    file_names = []
    for root, dirs, files in os.walk(f"{_path}"):
        for file in files:
            file_name = os.path.join(root, file).split('/')[-1]
            file_names.append(file_name)
    if _is_empty(file_types):
        return file_names
    return _filter(file_names, lambda file_name: file_name.split('.')[-1] in file_types)

def is_exist(_path: str):
    return os.path.exists(_path)

def concurrency(run, arr:list|tuple,count=2):
    start_time = time.time()
    length = len(arr)
    unit = round(length / count)
    max_index = length - 1
    index_list = []
    for value in range(0,count):
        if len(index_list) == 0:
            start_index = 0
        else:
            last_end_index = index_list[-1][1]
            start_index = last_end_index + 1
        if value == count - 1:
            end_index = max_index
        else:
            end_index = start_index + unit - 1
        index_list.append((start_index, end_index))
    print("concurrency segmentation",index_list)
    tlist: list[threading.Thread] = []
    for i, seg in enumerate(index_list):
        (start_index, end_index)=seg
        t = threading.Thread(
            target=run,
            args=(arr, start_index, end_index,)
        )
        tlist.append(t)

    for i,t in enumerate(tlist):
        t.daemon = True
        print(f"启动第{i+1}个线程")
        t.start()
    
    for i,t in enumerate(tlist):
        # print(f"第{i+1}个线程 join()")
        t.join()
    
    end_time = time.time()
    cost_time = f"{end_time-start_time:.2f}"
    print(f"cost-time：{cost_time}  concurrency done!! ")


def concurrency2(run, arr:list|tuple,count=2):
    start_time = time.time()
    length = len(arr)
    unit = round(length / count)
    max_index = length - 1
    index_list = []
    for value in range(0,count):
        if len(index_list) == 0:
            start_index = 0
        else:
            last_end_index = index_list[-1][1]
            start_index = last_end_index + 1
        if value == count - 1:
            end_index = max_index
        else:
            end_index = start_index + unit - 1
        index_list.append((start_index, end_index))
    print("concurrency segmentation",index_list)
    tlist: list[threading.Thread] = []
    for i, seg in enumerate(index_list):
        (start_index, end_index)=seg
        seg_arr = arr[start_index:end_index+1]
        t = threading.Thread(
            target=run,
            args=(seg_arr,)
        )
        tlist.append(t)

    for i,t in enumerate(tlist):
        t.daemon = True
        print(f"启动第{i+1}个线程")
        t.start()
    
    for i,t in enumerate(tlist):
        # print(f"第{i+1}个线程 join()")
        t.join()
    
    end_time = time.time()
    cost_time = f"{end_time-start_time:.2f}"
    print(f"cost-time：{cost_time}  concurrency done!! ")

# if __name__ == '__main__':
#     print(get_path('/model/tables.json'))

#     print(",".join(_map([1, 2, 3], lambda item: str(item))))

def get_median(iter:list[float]|tuple[float]):
    median_value = statistics.median(iter)
    return median_value

def find_annotations(table_json):
    '''
    查找是否有附注列（标志：附注）
    '''
    for row in table_json:
        if "附注" in row:
            return True
    

def supplementing_rows_by_max_length(table_json):
    '''
    根据json文件的item最大长度补充每一行列表（使表格对齐）
    在补充长度后，含附注的行长度与不含附注的行长度不一致----
        逻辑修改为：如果table内容中有匹配附注格式的，增加附注列后按max_length补充原row
    '''
    table_json_format = []
    max_length = max(_map(table_json, lambda row: len(row)))
    for item in table_json:
        index = 0 # 起始索引
        add_list = [] # 长度不一致的补充列表
        if find_annotations(table_json):
            if len(item) < 2: 
                item.insert(1,"")
        if len(item) < max_length:
            diff = max_length - len(item)
            while index < diff:
                add_list.append("")
                index = index + 1
            item = item + add_list
        table_json_format.append(item)
    return table_json_format


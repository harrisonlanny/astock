import time

import numpy
import numpy as np
import pandas as pd
from pandas import DataFrame, concat
from decimal import Decimal, getcontext
from datetime import date, timedelta, datetime

from db.str import safe_column, safe_field, safe_field_define
from ts.index import pro_bar, pro_api, fetch_daily, format_fields, fields_map_df, fetch_stock_basic_from_bs
from db.index import show_tables, delete_table, create_table, insert_table, read_table, describe_table, copy_table, \
    clear_table, update_table_fields, get_last_row, get_first_row, get_total
from model.index import describe_json
from model.model import TableModel
from utils.index import _map, parse_dataframe, print_dataframe, _map2, list2dict, add_date, add_date_str, str2date, \
    get_current_date, replace_nan_from_dataframe, _is_nan, get_path, _is_empty, get_dict_key_by_index
from utils.stock import fq, _filter
from service.index import api_query, get_current_d_tables, get_ts_code_from_symbol, update_d_tables
from ts.index import format_code
# import baostock as bs

# '600276.SH', '600276', '恒瑞医药', '江苏', '化学制药', '主板', 'L', datetime.date(2000, 10, 18)
# '002475.SZ', '002475', '立讯精密', '深圳', '元器件', '主板', 'L', datetime.date(2010, 9, 15)
# '688126.SH', '688126', '沪硅产业', '上海', '半导体', '科创板', 'L', datetime.date(2020, 4, 20)
# '600612.SH', '600612', '老凤祥', '上海', '服饰', '主板', 'L', datetime.date(1992, 8, 14)
# '600000.SH', '600000', '浦发银行', '上海', '银行', '主板', 'L', datetime.date(1999, 11, 10), None, 'H'


# row_list = read_table('d_688126', result_type='dict', filter_str="WHERE trade_date <= date'20201130'")
#
# adj_factors = _map(row_list, lambda row: row['adj_factor'])
#
# avg_af = numpy.mean(adj_factors)
# mid_af = numpy.median(adj_factors)
# target_af = adj_factors[-5:]
# print(avg_af, mid_af, target_af)

# test_list = [1,3,4,5,7,9,10,100]
# avg = numpy.mean(test_list)
# mid = numpy.median(test_list)
#
# print(avg,mid)

# print("哈哈".startswith("ST"))

# total = get_total('stock_basic', filter_str="WHERE `name` like '%药%'")
# print(total)

# print(format_code("000001.SZ", _to="bs"))
# fetch_stock_basic_from_bs()

import pdfplumber
import pandas as pd


# read_path = 'reports/lxjm.pdf'
# kw1 = "公允反映了"
# kw2 = "标准无保留意见"
# pdf_content = pdfplumber.open(read_path)
# result_df = pd.DataFrame()
# for page in pdf_content.pages:
#     table = page.extract_table()
#     df_detail = pd.DataFrame(table)
#     content = page.extract_text_simple(x_tolerance=3, y_tolerance=3)
#     if kw1 in content or kw2 in content:
#         print(1)

# 根据对角线坐标，得到rect的长宽
def get_size_by_points(points):
    if _is_empty(points) or len(points) != 4:
        return None
    top_left_point = {
        "x": points[0],
        "y": points[1]
    }
    right_bottom_point = {
        "x": points[2],
        "y": points[3]
    }
    width = right_bottom_point['x'] - top_left_point['x']
    height = right_bottom_point['y'] - top_left_point['y']

    return {
        "width": width,
        "height": height
    }


def is_cell_size_same(cell1, cell2, empty_is_same: bool = True):
    same_precision = 5
    size1 = get_size_by_points(cell1)
    size2 = get_size_by_points(cell2)

    is_cell1_empty = _is_empty(size1)
    is_cell2_empty = _is_empty(size2)

    if is_cell1_empty and is_cell2_empty:
        return True
    if is_cell1_empty or is_cell2_empty:
        return empty_is_same

    if abs(size1['width'] - size2['width']) <= same_precision:
        return True
    return False


def is_cells_size_same(cells1, cells2):
    if len(cells1) != len(cells2):
        return False
    for index, cell1 in enumerate(cells1):
        cell2 = cells2[index]
        if not is_cell_size_same(cell1, cell2):
            return False
    return True


def keep_visible_lines(obj):
    """
    If the object is a ``rect`` type, keep it only if the lines are visible.

    A visible line is the one having ``non_stroking_color`` as 0.
    """
    if obj['object_type'] == 'rect':
        return obj['non_stroking_color'] == 0
    return True


with pdfplumber.open('./reports/hgcy.pdf') as pdf:
    prev_table = None
    prev_table_id = None
    prev_table_index = None
    prev_table_count = None

    maybe_same_tables = {}
    _pages = pdf.pages
    for page_index, page in enumerate(_pages):
        print(f'---{page}---', '\n')
        # Filter out hidden lines.
        page = page.filter(keep_visible_lines)
        # im = page.to_image()
        # im.debug_tablefinder(tf={"vertical_strategy": 'lines', "horizontal_strategy": "lines"}).show()
        tables = page.find_tables()
        for table_index, table in enumerate(tables):
            if table_index == 2:
                print(table.extract())
            table_id = f"{page_index + 1}_{table_index + 1}"
            print(table_id, table.extract(), '\n')

            if prev_table:
                print('前一张表的最后一行', prev_table.rows[-1].cells)
                print('当前表的第一行', table.rows[0].cells, '\n')
                prev_table_last_row = prev_table.rows[-1]
                current_table_first_row = table.rows[0]

                # 如果列数不一致，那么就不是同结构
                # prev_table必须是上一页的最后一张表
                # current_table必须本页的第一张表
                if table_index == 0 and prev_table_index + 1 == prev_table_count and \
                        is_cells_size_same(prev_table_last_row.cells, current_table_first_row.cells):

                    # 判断table_id是否已存在于map中
                    key = get_dict_key_by_index(maybe_same_tables, -1)
                    if key is None or prev_table_id not in maybe_same_tables[key]:
                        if maybe_same_tables.get(prev_table_id) is None:
                            maybe_same_tables[prev_table_id] = [prev_table_id]
                        maybe_same_tables[prev_table_id].append(table_id)
                    else:
                        maybe_same_tables[key].append(table_id)

            prev_table = table
            prev_table_id = table_id
            prev_table_index = table_index
            prev_table_count = len(tables)
    print('可能是同一张表的map', maybe_same_tables)

# map = {}
# print(get_dict_key_by_index(map, -3))

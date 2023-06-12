from utils.index import _map, parse_dataframe, print_dataframe, _map2, list2dict, add_date, add_date_str, str2date, \
    get_current_date, replace_nan_from_dataframe, _is_nan, get_path, _is_empty, get_dict_key_by_index, txt, mul_str, \
    has_chinese_number, _find, json
from utils.stock import fq, _filter

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


def keep_bold_chars(obj):
    if obj['object_type'] == 'char':
        return 'Bold' in obj['fontname']
    return True


def page_obj(type: str, data, id: str = None):
    return {
        "type": type,
        "id": id,
        "data": data,
    }


def gen_table_model(id: str, range: list[str], name: str = None, desc: dict[str, list[str]] = None):
    return {
        "id": id,
        "range": range,
        "name": name,
        "desc": desc,
    }


def gen_table_id(page_ind: int, table_ind: int):
    return f"{page_ind + 1}_{table_ind + 1}"


# def is_datetime(data: str):
#


def gen_table_name(table_id: str, page_struct: any):
    # 获取去掉业眉的top数据
    top_data = _filter(get_top_table_data(table_id, page_struct)[1:], lambda item: item['type'] == 'text_line')
    for data in top_data[::-1]:

        text = data['data']['text'].strip()
        if has_chinese_number(text):
            return text
    return None


def parse_table_id(table_id: str):
    [page_num, table_num] = table_id.split("_")
    return {
        "page_num": int(page_num),
        "table_num": int(table_num)
    }


# 获取table上面的数据（限定在table所在页面内）
def get_top_table_data(table_id: str, page_struct: any):
    result = []
    page_num = parse_table_id(table_id)['page_num']
    current_page_struct = page_struct[page_num]
    for item in current_page_struct:
        if item['id'] == table_id and item['type'] == 'table':
            break
        else:
            result.append(item)
    return result


# 获取table下面的数据（限定在table所在页面内）
def get_bottom_table_data(table_id: str, page_struct: any):
    result = []
    page_num = parse_table_id(table_id)['page_num']
    current_page_struct = page_struct[page_num]
    should_collect = False
    for item in current_page_struct:
        if item['id'] == table_id and item['type'] == 'table':
            should_collect = True
        if item['type'] != 'table' and should_collect:
            result.append(item)
    return result


GAP = 17


def get_table_top_desc(table_id: str, page_struct: any):
    result = []
    page_num = parse_table_id(table_id)['page_num']
    table = _find(page_struct[page_num], lambda item: item['id'] == table_id)
    top_data = _filter(get_top_table_data(table_id, page_struct)[1:], lambda item: item['type'] == 'text_line')

    [t_x0, t_top, t_x1, t_bottom] = table['data'].bbox
    prev_item = {
        "x0": t_x0,
        "x1": t_x1,
        "top": t_top,
        "bottom": t_bottom
    }
    # 算出每个item的间距（第一个是table，然后是最靠着table的text，依次外推）
    for index, item in enumerate(top_data[::-1]):
        gap = prev_item['top'] - item['data']['bottom']
        if gap <= GAP:
            # print(table_id, 'top', gap)
            result.append(item)
        else:
            break
        prev_item = item['data']
    result.reverse()
    return result


def get_table_bottom_desc(table_id: str, page_struct: any):
    result = []
    page_num = parse_table_id(table_id)['page_num']
    table = _find(page_struct[page_num], lambda item: item['id'] == table_id)
    bottom_data = _filter(get_bottom_table_data(table_id, page_struct)[:-1], lambda item: item['type'] == 'text_line')

    [t_x0, t_top, t_x1, t_bottom] = table['data'].bbox
    prev_item = {
        "x0": t_x0,
        "x1": t_x1,
        "top": t_top,
        "bottom": t_bottom
    }

    for index, item in enumerate(bottom_data):
        gap = item['data']['top'] - prev_item['bottom']
        if gap <= GAP:
            # print(table_id, 'bottom', gap, item['data']['text'])
            result.append(item)
        else:
            break
        prev_item = item['data']

    return result


def get_table_desc(table_id: str, page_struct: any):
    top_desc = get_table_top_desc(table_id, page_struct)
    bottom_desc = get_table_bottom_desc(table_id, page_struct)

    top_desc_list = _map(top_desc, lambda item: item['data']['text'])
    bottom_desc_list = _map(bottom_desc, lambda item: item['data']['text'])

    return {
        "top": top_desc_list,
        "bottom": bottom_desc_list
    }


# def parse_table_desc(table_desc: dict[str, list[str]]):
#     top_desc =table_desc['top'][::-1]
#     for text in top_desc:
#         if ['单位']

def parse_pdf():
    with pdfplumber.open('./reports/hgcy.pdf') as pdf:
        prev_table = None
        prev_table_id = None
        prev_table_index = None
        prev_table_count = None

        page_struct = {}
        maybe_same_tables = {}
        single_tables = {}
        # _pages = [pdf.pages[150], pdf.pages[151]]
        _pages = pdf.pages
        table_id_list = []
        for page_index, page in enumerate(_pages):
            print(f'---{page}---', '\n')
            # Filter out hidden lines.
            page = page.filter(keep_visible_lines)
            tables = page.find_tables()
            # im = page.to_image()
            # im.debug_tablefinder(tf={"vertical_strategy": 'lines', "horizontal_strategy": "lines"}).show()

            # 提取页面的文字
            text_lines = page.extract_text_lines()

            # 确定页面里文字和表格关系
            if page_struct.get(page_index + 1) is None:
                page_struct[page_index + 1] = []
                current_page_struct = page_struct[page_index + 1]
            table_index = 0
            for text_line in text_lines:
                top = text_line['top']
                bottom = text_line['bottom']
                x0 = text_line['x0']
                x1 = text_line['x1']

                if table_index <= len(tables) - 1:
                    table = tables[table_index]
                    [t_x0, t_top, t_x1, t_bottom] = table.bbox
                    # 在表格上方
                    if bottom < t_top:
                        current_page_struct.append(page_obj("text_line", text_line))
                    # 在表格下方
                    elif top > t_bottom:
                        current_page_struct.append(page_obj("table", table, gen_table_id(page_index, table_index)))
                        current_page_struct.append(page_obj("text_line", text_line))
                        table_index += 1
                else:
                    current_page_struct.append(page_obj("text_line", text_line))

            for table_index, table in enumerate(tables):
                table_id = gen_table_id(page_index, table_index)
                table_id_list.append(table_id)

                # 不同页面的相同表合并
                if prev_table:
                    print('前一张表的最后一行', prev_table.rows[-1].cells)
                    print('当前表的第一行', table.rows[0].cells, '\n')
                    prev_table_last_row = prev_table.rows[-1]
                    current_table_first_row = table.rows[0]

                    # 如果列数不一致，那么就不是同结构
                    # prev_table必须是上一页的最后一张表
                    # current_table必须本页的第一张表
                    if table_index == 0 and prev_table_index + 1 == prev_table_count and \
                            len(get_top_table_data(table_id, page_struct) + get_bottom_table_data(prev_table_id,
                                                                                                  page_struct)) <= 2 and \
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

        # 将maybe_same_tables降为一维数组
        same_tables = []
        for key in maybe_same_tables:
            same_tables += maybe_same_tables[key]

        # 将逻辑统一的tables和单独的tables归纳到一起 ['1-1', ['2-1','2-2'], ...]
        all_tables = []
        for table_id in table_id_list:
            if table_id not in same_tables:
                # 获取table上面的text_lines，从而给出一个推测的name
                # table_name = gen_table_name(table_id, page_struct)
                table_desc = get_table_desc(table_id, page_struct)
                all_tables.append(gen_table_model(table_id, [table_id], desc=table_desc))
            else:
                same_ids = maybe_same_tables.get(table_id)
                # 为空说明不是逻辑表的第一张表，所以不需要管，不为空说明是头表，直接加入all_tables即可
                if same_ids is not None:
                    # 获取table上面的text_lines，从而给出一个推测的name
                    # table_name = gen_table_name(table_id, page_struct)
                    table_desc = get_table_desc(table_id, page_struct)
                    all_tables.append(gen_table_model(table_id, same_ids, desc=table_desc))
        # txt('/all_tables.txt', all_tables)
        json('/all_tables.json', all_tables)


all_tables = json('/all_tables.json')
table = _filter(all_tables, lambda table: "合并资产负债表" in table["desc"]["top"])
txt('/target.txt', table)

import math
import os
import threading
import time
from enum import Enum

import pdfplumber
import requests
from pdfplumber.table import Table

from db.index import clear_table, insert_table, get_total, delete_rows, read_table, update_table
from model.index import describe_json
from service.index import get_current_d_tables
from utils.index import _is_number, json, _filter, date2str, is_iterable, _map, get_path, _is_empty, get_dict_key_by_index, _find, \
    large_num_format, has_chinese_number
from datetime import date

STATIC_ANNOUNCEMENTS_DIR = '/static/announcements'
STATIC_ANNOUNCEMENTS_PARSE_DIR = '/static/parse-announcements'

JU_CHAO_PROTOCOL = "http://"
JU_CHAO_HOST = 'www.cninfo.com.cn'
JU_CHAO_STATIC_HOST = 'static.cninfo.com.cn'
JU_CHAO_BASE_URL = f'{JU_CHAO_PROTOCOL}{JU_CHAO_HOST}'
JU_CHAO_STATIC_URL = f'{JU_CHAO_PROTOCOL}{JU_CHAO_STATIC_HOST}'

JU_CHAO_COOKIE = {
    "JSESSIONID": "7BE08DF899613177F426827B251A630E",
    "insert_cookie": "45380249",
    "routeId": ".uc1",
    "_sp_ses.2141": "*",
    "_sp_id.2141": "69ba6006-a96c-4e68-8da2-1f3a56e943f2.1685969405.4.1686988217.1686383617.1042ce01-5319-4d64-a642-8b6dbd468a75",
}
JU_CHAO_HEADERS = {
    "Host": "www.cninfo.com.cn",
    "Origin": "http://www.cninfo.com.cn",
    "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&lastPage=index",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}


def fetch_juchao_all_stocks(refresh: bool = True):
    if refresh:
        response = requests.get(f'{JU_CHAO_BASE_URL}/new/data/szse_stock.json', cookies=JU_CHAO_COOKIE,
                                headers=JU_CHAO_HEADERS)
        json(f'{STATIC_ANNOUNCEMENTS_DIR}/juchao_all_stocks.json', response.json())
        return response.json()['stockList']
    return json(f'{STATIC_ANNOUNCEMENTS_DIR}/juchao_all_stocks.json')['stockList']


def fetch_one_page_financial_statements(symbol: str, org_id: str, page_num: int, page_size: int):
    try:
        response = requests.post(f'{JU_CHAO_BASE_URL}/new/hisAnnouncement/query', data={
            "pageNum": page_num,
            "pageSize": page_size,
            "column": "szse",
            "tabName": "fulltext",
            "stock": f"{symbol},{org_id}",
            "category": "category_ndbg_szsh",
            "isHLtitle": True
        }, cookies=JU_CHAO_COOKIE, headers=dict(JU_CHAO_HEADERS, **{
            "Content-Type": "application/x-www-form-urlencoded"
        }))
        result = response.json()
        return result
    except requests.exceptions.ConnectionError:
        print('请求one_page_financeial_statements超负荷了，休息15s后将继续请求..')
        time.sleep(15)
        return fetch_one_page_financial_statements(symbol, org_id, page_num, page_size)


def fetch_financial_statements(symbol: str, org_id: str):
    page_num = 1
    page_size = 30
    result = []
    has_more = True

    while has_more:
        page_result = fetch_one_page_financial_statements(symbol, org_id, page_num, page_size)
        print(f"{symbol}的第{page_num}页: ", page_result)
        if is_iterable(page_result['announcements']):
            result += page_result['announcements']
        has_more = page_result['hasMore']
        page_num += 1
    return result


# 沪硅产业2022年年度报告摘要
# finalpage/2023-04-11/1216371290.PDF
# "/new/disclosure/detail?stockCode=688126&amp;announcementId=1216371290&amp;orgId=9900039304&amp;announcementTime=2023-04-11"
# http://static.cninfo.com.cn/finalpage/2023-04-11/1216371290.PDF
def get_pdf_url(announcement, simple: bool = True):
    if simple:
        return f'{JU_CHAO_STATIC_URL}/{announcement["adjunctUrl"]}'

    announcement_time = date2str(date.fromtimestamp(announcement['announcementTime'] / 1000), "%Y-%m-%d")
    return f'/new/disclosure/detail?stockCode={announcement["secCode"]}&amp;announcementId={announcement["announcementId"]}&amp;orgId={announcement["orgId"]}&amp;announcementTime={announcement_time}'


class DownLoadAnnouncementException(Exception):
    def __init__(self, code, reason, url):
        self.code = code
        self.reason = reason
        self.url = url

    def __str__(self):
        return f"获取announcement报错：{self.url},{self.code},{self.reason}"


def download_announcement(url: str, title: str, skip_if_exist: bool = True):
    save_path = get_path(STATIC_ANNOUNCEMENTS_DIR) + "/" + title + '.pdf'
    print('save_path', save_path)
    if skip_if_exist:
        # 判断下 title.pdf是否已经存在于静态文件夹，如果已经存在就直接return，不做操作
        is_exist = os.path.exists(save_path)

        if is_exist:
            file_size = os.path.getsize(save_path)
            # 如果大小小于1kb，我们认为是空文件
            if file_size >= 1024:
                print(f"{title}.pdf 已存在，不会重复下载 ")
                return is_exist
    try:
        response = requests.get(url)
    except requests.exceptions.ConnectionError:
        print('请求pdf超负荷了，休息15s后将继续请求..')
        time.sleep(15)
        response = requests.get(url)
    if response.status_code != 200:
        raise DownLoadAnnouncementException(
            code=response.status_code,
            reason=response.reason,
            url=url
        )
    # wb指的是write_byte,以二进制形式写入
    with open(save_path, 'wb') as f:
        f.write(response.content)
    return False


# if __name__ == '__main__':


def refresh_table_announcements(symbols: list[str] = None, start_symbol: str = None, sleep_time: float = 0.5):
    # 1. 得到symbols
    if _is_empty(symbols):
        symbols = _map(get_current_d_tables(), lambda item: item[2:])
    # 如果有start_symbol,就从start_symbol开始截取symbols（用于因为异常中断后，避免重新开始更新，而是从中断处开始）
    if not _is_empty(start_symbol):
        _symbols = []
        should_add = False
        for symbol in symbols:
            if symbol == start_symbol:
                should_add = True
            if should_add:
                _symbols.append(symbol)
        symbols = _symbols
    print(symbols)

    # 2. 得到巨潮的all_stocks (包含org_id与symbol的映射)
    juchao_all_stocks = fetch_juchao_all_stocks()

    # 3. 根据symbols和all_stocks得到包含org_id和symbol的dictlist
    target_juchao_stocks = _filter(juchao_all_stocks, lambda item: item['code'] in symbols)

    # 4. 获取announcements表的描述信息
    table_name = 'announcements'
    table_describe = describe_json(table_name)

    # 遍历target_juchao_stocks
    for stock_index, stock in enumerate(target_juchao_stocks):
        percent = round((stock_index + 1) / len(target_juchao_stocks) * 100, 2)
        print(stock['zwjc'], f"{percent}%")
        name = stock['zwjc']
        org_id = stock['orgId']
        symbol = stock['code']
        # 请求symbol对应的所有财报
        announcements = fetch_financial_statements(symbol, org_id)

        # 将数据结构化 （format_item指的是symbol的所有财报数据)
        # rows: [[symbol, name, org_id, file_title, title, url]]
        rows = []
        format_item = {
            "symbol": symbol,
            "name": name,
            "org_id": org_id,
            "announcements": []
        }
        # dict[file_title, count]
        same_file_title_count = {}

        for announcement in announcements:
            pdf_title = announcement['announcementTitle'].strip()
            pdf_url = get_pdf_url(announcement)
            announcement_id = announcement['announcementId']
            file_title = f"{symbol}__{name}__{pdf_title}__{announcement_id}"
            format_item['announcements'].append({
                "file_title": file_title,
                "title": pdf_title,
                "url": pdf_url
            })

            # 在same_file_title_count里添上记录
            if same_file_title_count.get(file_title) is None:
                same_file_title_count[file_title] = 1
            else:
                same_file_title_count[file_title] += 1

            # 如果same_file_title_count里统计>=2（不止一条），那么在file_title的末尾添加上__{count-1}
            if same_file_title_count[file_title] >= 2:
                file_title = f"{file_title}__{same_file_title_count[file_title] - 1}"

            row = [symbol, name, org_id, file_title, pdf_title, pdf_url]
            rows.append(row)
            # 下载对应的pdf，存储到静态文件夹
            print(f'{file_title}.pdf is downloading...')
            is_exist = download_announcement(pdf_url, file_title)
            if not is_exist:
                time.sleep(sleep_time)

        if len(rows):
            # 删除原数据库中symbol对应的所有announcements
            delete_rows(table_name, f"WHERE `symbol`={symbol}")
            # 将rows插入数据库
            print('insert table')
            for row_index, row in enumerate(rows):
                print(f"第{row_index}行", row)
            insert_table(table_name, table_describe.safe_column_names, rows)
        print('\n')
        time.sleep(sleep_time)


def get_year_announcements(year: int = None):
    year_filter = '' if year is None else f' title like ' % {year} % ' and'
    result = read_table(table_name="announcements",
                        filter_str=f"where{year_filter} title not like '%英文%'  and title not like "
                                   f"'%取消%' and title not like '%摘要%'",
                        result_type="dict")
    return result


def download_year_announcements(year: int = None):
    announcements: list[dict] = get_year_announcements(year)
    # announcements = announcements[0:300]
    for index, announcement in enumerate(announcements):
        percent = round((index + 1) / len(announcements) * 100, 2)

        symbol = announcement['symbol']
        url = announcement['url']
        file_title = announcement['file_title']
        print(file_title, f"{percent}%")
        try:
            download_announcement(url, file_title)
        except DownLoadAnnouncementException as e:
            print(e)
            # 如果pdf404，则在announcements表中标记为不可用
            if e.code == 404:
                update_table("announcements", ['`disabled`'], [1], f"WHERE `file_title`='{file_title}'")


def page_obj(type: str, data, id: str = None):
    return {
        "type": type,
        "id": id,
        "data": data,
    }


def keep_bold_chars(obj):
    if obj['object_type'] == 'char':
        return 'Bold' in obj['fontname']
    return True

def gen_table_id(page_ind: int, table_ind: int):
    return f"{page_ind + 1}_{table_ind + 1}"


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


def get_table_top_desc(table_id: str, page_struct: any):
    result = []
    page_num = parse_table_id(table_id)['page_num']
    table = _find(page_struct[page_num], lambda item: item['id'] == table_id)
    top_data = _filter(get_top_table_data(table_id, page_struct)[1:], lambda item: item['type'] == 'text_line')

    print(f"table_id: {table_id}, page_num: {page_num}")
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


GAP = 17


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


def gen_table_model(id: str, range: list[str], name: str = None, desc: dict[str, list[str]] = None):
    return {
        "id": id,
        "range": range,
        "name": name,
        "desc": desc,
    }


def find_table_from_page_struct(table_id: str, page_struct: dict[int, list[dict]]):
    page_num = parse_table_id(table_id)['page_num']
    page_data = page_struct[page_num]

    table = _find(page_data, lambda item: item['type'] == 'table')
    return table


def gen_table_name(table_id: str, page_struct: any):
    # 获取去掉页眉的top数据
    top_data = _filter(get_top_table_data(table_id, page_struct)[1:], lambda item: item['type'] == 'text_line')
    for data in top_data[::-1]:

        text = data['data']['text'].strip()
        if has_chinese_number(text):
            return text
    return None


def gen_table_content_model(id: str):
    return {
        "id": id,
        "data": {
            "fields": [],
            "rows": [],
            "origin": any
        }
    }


class Financial_Statement(Enum):
    合并资产负债表 = "合并资产负债表"


# 输出 1. base.json 2.table.json 3.财务报表.json
def parse_pdf(pdf_url, pdf_name):
    with pdfplumber.open(pdf_url) as pdf:
        table_json_url = f"{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{pdf_name}__table.json"
        base_json_url = f"{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{pdf_name}__base.json"
        hbzcfzb_json_url = f"{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{pdf_name}__{Financial_Statement.合并资产负债表.value}.json"

        prev_table = None
        prev_table_id = None
        prev_table_index = None
        prev_table_count = None

        page_struct = {}
        maybe_same_tables = {}
        # _pages = [pdf.pages[150], pdf.pages[151]]
        _pages = pdf.pages[150:151]
        # _pages = pdf.pages[26:27]
        # _pages = pdf.pages
        table_id_list = []
        pdf_line_rect_result = {
            "name": pdf_name,
            "line": {
                "count": 0,
                "color": {},
                "max_color": None
            },
            "rect": {
                "count": 0,
                "color": {},
                "max_color": None
            },
        }

        def color_distribution(obj):
            if obj['object_type'] == 'rect':
                # 获取颜色
                color = obj['non_stroking_color']
                if isinstance(color, list):
                    color = tuple(color)
                # 计数+1
                pdf_line_rect_result['rect']['count'] += 1
                # 颜色统计+1
                if pdf_line_rect_result['rect']['color'].get(color) is None:
                    pdf_line_rect_result['rect']['color'][color] = 0
                pdf_line_rect_result['rect']['color'][color] += 1

            if obj['object_type'] == 'line':
                # 获取颜色
                color = obj['stroking_color']
                if isinstance(color, list):
                    color = tuple(color)
                # 计数+1
                pdf_line_rect_result['line']['count'] += 1
                # 颜色统计+1
                if pdf_line_rect_result['line']['color'].get(color) is None:
                    pdf_line_rect_result['line']['color'][color] = 0
                pdf_line_rect_result['line']['color'][color] += 1
                
            return True
        
        def get_max_color(color_result):
            max_color = None
            max_count = 0
            for color in color_result:
                color_count = color_result[color]
                if max_count == 0 or (color_count > max_count):
                    max_count = color_count
                    max_color = color
            return max_color

        def keep_visible_lines(obj):
            if obj['object_type'] == 'rect':
                line_max_color = pdf_line_rect_result['line']['max_color']
                color = obj['non_stroking_color']

                # 统一为tuple 再进行比较
                if _is_number(color):
                    color = (color, color, color)
                if _is_number(line_max_color):
                    line_max_color = (line_max_color, line_max_color, line_max_color)
                
                # 如果颜色和line最多色一致，则视为可见，否则不可见
                return color == line_max_color
            return True
        
        # 全页面颜色扫描
        for page_index, page in enumerate(pdf.pages):
            print(f'color_scan --{pdf_name}  {page_index + 1}/{len(pdf.pages)}--', '\n')
            page = page.filter(color_distribution)
            page.find_tables()

        pdf_line_rect_result['line']['max_color'] = get_max_color(pdf_line_rect_result['line']['color'])
        pdf_line_rect_result['rect']['max_color'] = get_max_color(pdf_line_rect_result['rect']['color'])
        print('color_result: ', pdf_line_rect_result)



        return pdf_line_rect_result

        # 数据提取 
        for page_index, page in enumerate(_pages):
            print(f'--{pdf_name}  {page_index + 1}/{len(_pages)}--', '\n')
            # Filter out hidden lines.
            page = page.filter(keep_visible_lines)
            tables = page.find_tables()
            # print(len(tables))
            im = page.to_image()
            im.debug_tablefinder(tf={"vertical_strategy": 'lines', "horizontal_strategy": "lines"}).show()

            continue

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
                    # print('前一张表的最后一行', prev_table.rows[-1].cells)
                    # print('当前表的第一行', table.rows[0].cells, '\n')
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
                        # print(
                        #     f"table_id:{table_id};prev_table_id:{prev_table_id},key:{key}, maybe_same_tables:{maybe_same_tables}")
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

        # json('maybe_same_tables.json', maybe_same_tables)

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
                    table_top_desc = get_table_desc(table_id, page_struct)['top']
                    table_bottom_desc = get_table_desc(same_ids[-1], page_struct)['bottom']
                    table_desc = {
                        "top": table_top_desc,
                        "bottom": table_bottom_desc
                    }
                    all_tables.append(gen_table_model(table_id, same_ids, desc=table_desc))
        


        for table in all_tables:
            table_id = table['id']
            range = table['range']

            table_extracts = []

            for id in range:
                table_detail: Table = find_table_from_page_struct(id, page_struct)['data']
                table_extract = table_detail.extract()
                format_extract = []
                for row in table_extract:
                    format_row = []
                    for cell in row:
                        cell = large_num_format(cell)
                        format_row.append(cell)
                    format_extract.append(format_row)
                table_extracts += format_extract

            if Financial_Statement.合并资产负债表.value in table['desc']['top']:
                # print(Financial_Statement.合并资产负债表.value, table_extracts)
                # json(Financial_Statement.合并资产负债表.value + '.json', table_extracts)
                json(hbzcfzb_json_url, table_extracts)
        # 输出json
        json(table_json_url, all_tables)
        # page_struct中的table替换为table.extract的文本
        for page_num in page_struct:
            page_info_list = page_struct[page_num]
            for item in page_info_list:
                if item['type'] == 'table':
                    item['data'] = item['data'].extract()
        json(base_json_url, page_struct)

        # return all_tables


def get_color_statistic(pdf_url):


    result = {}

    def page_filter(obj):
        if obj['object_type'] == 'rect':
            non_stroking_color = obj['non_stroking_color']
            if isinstance(non_stroking_color, list):
                non_stroking_color = tuple(non_stroking_color)
            if result.get(non_stroking_color) is None:
                result[non_stroking_color] = 0
            result[non_stroking_color] += 1
        return True

    with pdfplumber.open(pdf_url) as pdf:
        # print("pdf_url:", pdf_url)
        _pages = pdf.pages
        for page_index, page in enumerate(_pages):
            page = page.filter(page_filter)
            # 懒加载
            tables = page.find_tables()
        print(result)
        return result

# 2010, 2020

class ParsePdfThread(threading.Thread):
    def __init__(self,thread_name, file_title_list, start_index, end_index):
        self.thread_name = thread_name
        self.file_title_list = file_title_list
        self.start_index = start_index
        self.end_index = end_index
        super().__init__()

    def run(self):
        arr = self.file_title_list[self.start_index:self.end_index+1]
        arr_length = len(arr)
        for i,file_title in enumerate(arr):
            index = self.start_index + i
            pdf_url = get_path(f'{STATIC_ANNOUNCEMENTS_DIR}/{file_title}.pdf')
            table_json_url = get_path(f'{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{file_title}__table.json')
            print(f"{self.thread_name}: {file_title}, 线程内进度：{i+1}/{arr_length}, 总任务第几个：{index + 1}")
            exists = os.path.exists(table_json_url)
            if not exists:
                parse_pdf(pdf_url, pdf_name=file_title)


# def parse_pdfs(file_title_list):
#     length = len(file_title_list)
#     for index, file_title in enumerate(file_title_list):
#         pdf_url = get_path(f'{STATIC_ANNOUNCEMENTS_DIR}/{file_title}.pdf')
#         table_json_url = get_path(f'{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{file_title}__table.json')
#         print(pdf_url, f" {index + 1}/{length}")
#         exists = os.path.exists(table_json_url)
#         if not exists:
#             parse_pdf(pdf_url, pdf_name=file_title)

def parse_announcements(start_year,end_year):
    point_year = start_year
    or_str = ""
    while point_year <= end_year:
        suffix = " or " if point_year < end_year else ""
        or_str += f'title like "%{point_year}%"{suffix}'
        point_year += 1

    r = read_table(table_name="announcements",
                fields=["file_title"],
                result_type="dict",
                filter_str=
                f'''
                    where title not like "%英文%" and 
                    title not like "%取消%" and 
                    title not like "%摘要%" and 
                    title not like "%公告%" and
                    (
                        {or_str}
                    )
                ''')
    file_title_list = _map(r, lambda item: item['file_title'])
    middle = math.ceil(len(file_title_list) / 2)
    t1 = ParsePdfThread(
        thread_name="线程1",
        file_title_list=file_title_list,
        start_index=0,
        end_index=middle
    )
    t1.start()
    t2 = ParsePdfThread(
        thread_name="线程2",
        file_title_list=file_title_list,
        start_index=middle+1,
        end_index=len(file_title_list)-1
    )
    t2.start()

    print(f"多线程解析{start_year}-{end_year}年报完成")



import math
import os
import threading
import time

import pdfplumber
import requests
from pdfplumber.table import Table
from service.config import (
    JU_CHAO_BASE_URL,
    JU_CHAO_COOKIE,
    JU_CHAO_HEADERS,
    JU_CHAO_STATIC_URL,
    STATIC_ANNOUNCEMENTS_DIR,
    STATIC_ANNOUNCEMENTS_PARSE_DIR,
    Announcement_Category,
    Announcement_Category_Options,
    Financial_Statement,
)

from db.index import (
    clear_table,
    insert_table,
    get_total,
    delete_rows,
    read_table,
    update_table,
)
from model.index import describe_json
from service.index import get_current_d_tables
from utils.index import (
    _every,
    _is_number,
    _safe_join,
    has_english_number,
    is_exist,
    is_list_item_same,
    json,
    _filter,
    date2str,
    is_iterable,
    _map,
    get_path,
    _is_empty,
    get_dict_key_by_index,
    _find,
    json2,
    large_num_format,
    has_chinese_number,
)
from datetime import date


def fetch_juchao_all_stocks(refresh: bool = True):
    if refresh:
        response = requests.get(
            f"{JU_CHAO_BASE_URL}/new/data/szse_stock.json",
            cookies=JU_CHAO_COOKIE,
            headers=JU_CHAO_HEADERS,
        )
        json(f"{STATIC_ANNOUNCEMENTS_DIR}/juchao_all_stocks.json", response.json())
        return response.json()["stockList"]
    return json(f"{STATIC_ANNOUNCEMENTS_DIR}/juchao_all_stocks.json")["stockList"]


def fetch_one_page_financial_statements(
    symbol: str,
    org_id: str,
    announcement_category: list[Announcement_Category],
    page_num: int,
    page_size: int,
):
    try:
        ann_category_option_list = _map(announcement_category, lambda item: Announcement_Category_Options[item.value])
        juchao_category_list = _map(ann_category_option_list, lambda item: item['juchao_category'])
        juchao_category_str = _safe_join(juchao_category_list, ";")
        response = requests.post(
            f"{JU_CHAO_BASE_URL}/new/hisAnnouncement/query",
            data={
                "pageNum": page_num,
                "pageSize": page_size,
                "column": "szse",
                "tabName": "fulltext",
                "stock": f"{symbol},{org_id}",
                "category": juchao_category_str,
                "isHLtitle": True,
            },
            cookies=JU_CHAO_COOKIE,
            headers=dict(
                JU_CHAO_HEADERS, **{"Content-Type": "application/x-www-form-urlencoded"}
            ),
        )
        result = response.json()
        return result
    except requests.exceptions.ConnectionError:
        print("请求one_page_financeial_statements超负荷了，休息15s后将继续请求..")
        time.sleep(15)
        return fetch_one_page_financial_statements(symbol,org_id,announcement_category,page_num,page_size)


def fetch_financial_statements(symbol: str, org_id: str, announcement_category: list[Announcement_Category]):
    page_num = 1
    page_size = 30
    result = []
    has_more = True

    while has_more:
        page_result = fetch_one_page_financial_statements(
            symbol, org_id,announcement_category, page_num, page_size
        )
        print(f"{symbol}的第{page_num}页: ", page_result)
        if is_iterable(page_result["announcements"]):
            result += page_result["announcements"]
        has_more = page_result["hasMore"]
        page_num += 1
    return result


# 沪硅产业2022年年度报告摘要
# finalpage/2023-04-11/1216371290.PDF
# "/new/disclosure/detail?stockCode=688126&amp;announcementId=1216371290&amp;orgId=9900039304&amp;announcementTime=2023-04-11"
# http://static.cninfo.com.cn/finalpage/2023-04-11/1216371290.PDF
def get_pdf_url(announcement, simple: bool = True):
    if simple:
        return f'{JU_CHAO_STATIC_URL}/{announcement["adjunctUrl"]}'

    announcement_time = date2str(
        date.fromtimestamp(announcement["announcementTime"] / 1000), "%Y-%m-%d"
    )
    return f'/new/disclosure/detail?stockCode={announcement["secCode"]}&amp;announcementId={announcement["announcementId"]}&amp;orgId={announcement["orgId"]}&amp;announcementTime={announcement_time}'


class DownLoadAnnouncementException(Exception):
    def __init__(self, code, reason, url):
        self.code = code
        self.reason = reason
        self.url = url

    def __str__(self):
        return f"获取announcement报错：{self.url},{self.code},{self.reason}"


def download_announcement(url: str, title: str, skip_if_exist: bool = True):
    save_path = get_path(STATIC_ANNOUNCEMENTS_DIR) + "/" + title + ".pdf"
    print("save_path", save_path)
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
        print("请求pdf超负荷了，休息15s后将继续请求..")
        time.sleep(15)
        response = requests.get(url)
    if response.status_code != 200:
        raise DownLoadAnnouncementException(
            code=response.status_code, reason=response.reason, url=url
        )
    # wb指的是write_byte,以二进制形式写入
    with open(save_path, "wb") as f:
        f.write(response.content)
    return False


# if __name__ == '__main__':


def refresh_table_announcements(
    symbols: list[str] = None,
    announcement_category: list[Announcement_Category] = [
        Announcement_Category.一季报,
        Announcement_Category.半年报,
        Announcement_Category.三季报,
        Announcement_Category.年报
    ],
    start_symbol: str = None, 
    sleep_time: float = 0.5
):
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
    target_juchao_stocks = _filter(
        juchao_all_stocks, lambda item: item["code"] in symbols
    )

    # 4. 获取announcements表的描述信息
    table_name = "announcements"
    table_describe = describe_json(table_name)

    # 遍历target_juchao_stocks
    for stock_index, stock in enumerate(target_juchao_stocks):
        percent = round((stock_index + 1) / len(target_juchao_stocks) * 100, 2)
        print(stock["zwjc"], f"{percent}%")
        name = stock["zwjc"]
        org_id = stock["orgId"]
        symbol = stock["code"]
        # 请求symbol对应的所有财报
        announcements = fetch_financial_statements(symbol, org_id, announcement_category)

        # 将数据结构化 （format_item指的是symbol的所有财报数据)
        # rows: [[symbol, name, org_id, file_title, title, url]]
        rows = []
        format_item = {
            "symbol": symbol,
            "name": name,
            "org_id": org_id,
            "announcements": [],
        }
        # dict[file_title, count]
        same_file_title_count = {}

        for announcement in announcements:
            pdf_title = announcement["announcementTitle"].strip()
            pdf_url = get_pdf_url(announcement)
            announcement_id = announcement["announcementId"]
            file_title = f"{symbol}__{name}__{pdf_title}__{announcement_id}"
            format_item["announcements"].append(
                {"file_title": file_title, "title": pdf_title, "url": pdf_url}
            )

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
            print(f"{file_title}.pdf is downloading...")
            is_exist = download_announcement(pdf_url, file_title)
            if not is_exist:
                time.sleep(sleep_time)

        if len(rows):
            # 删除原数据库中symbol对应的所有announcements
            delete_rows(table_name, f"WHERE `symbol`={symbol}")
            # 将rows插入数据库
            print("insert table")
            for row_index, row in enumerate(rows):
                print(f"第{row_index}行", row)
            insert_table(table_name, table_describe.safe_column_names, rows)
        print("\n")
        time.sleep(sleep_time)


def get_year_announcements(year: int = None):
    year_filter = "" if year is None else f" title like " % {year} % " and"
    result = read_table(
        table_name="announcements",
        filter_str=f"where{year_filter} title not like '%英文%'  and title not like "
        f"'%取消%' and title not like '%摘要%'",
        result_type="dict",
    )
    return result


def download_year_announcements(year: int = None):
    announcements: list[dict] = get_year_announcements(year)
    # announcements = announcements[0:300]
    for index, announcement in enumerate(announcements):
        percent = round((index + 1) / len(announcements) * 100, 2)

        symbol = announcement["symbol"]
        url = announcement["url"]
        file_title = announcement["file_title"]
        print(file_title, f"{percent}%")
        try:
            download_announcement(url, file_title)
        except DownLoadAnnouncementException as e:
            print(e)
            # 如果pdf404，则在announcements表中标记为不可用
            if e.code == 404:
                update_table(
                    "announcements",
                    ["`disabled`"],
                    [1],
                    f"WHERE `file_title`='{file_title}'",
                )


def page_obj(type: str, data, id: str = None):
    return {
        "type": type,
        "id": id,
        "data": data,
    }


def keep_bold_chars(obj):
    if obj["object_type"] == "char":
        return "Bold" in obj["fontname"]
    return True


def gen_table_id(page_ind: int, table_ind: int):
    return f"{page_ind + 1}_{table_ind + 1}"


def parse_table_id(table_id: str):
    [page_num, table_num] = table_id.split("_")
    return {"page_num": int(page_num), "table_num": int(table_num)}


# 获取table上面的数据（限定在table所在页面内）
def get_top_table_data(table_id: str, page_struct: any):
    result = []
    page_num = parse_table_id(table_id)["page_num"]
    current_page_struct = page_struct[page_num]
    for item in current_page_struct:
        if item["id"] == table_id and item["type"] == "table":
            break
        else:
            result.append(item)
    return result


# 获取table下面的数据（限定在table所在页面内）
def get_bottom_table_data(table_id: str, page_struct: any):
    result = []
    page_num = parse_table_id(table_id)["page_num"]
    current_page_struct = page_struct[page_num]
    should_collect = False
    for item in current_page_struct:
        if item["id"] == table_id and item["type"] == "table":
            should_collect = True
        if item["type"] != "table" and should_collect:
            result.append(item)
    return result


# 根据对角线坐标，得到rect的长宽
def get_size_by_points(points):
    if _is_empty(points) or len(points) != 4:
        return None
    top_left_point = {"x": points[0], "y": points[1]}
    right_bottom_point = {"x": points[2], "y": points[3]}
    width = right_bottom_point["x"] - top_left_point["x"]
    height = right_bottom_point["y"] - top_left_point["y"]

    return {"width": width, "height": height}


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

    if abs(size1["width"] - size2["width"]) <= same_precision:
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
    page_num = parse_table_id(table_id)["page_num"]
    table = _find(page_struct[page_num], lambda item: item["id"] == table_id)
    top_data = _filter(
        get_top_table_data(table_id, page_struct)[1:],
        lambda item: item["type"] == "text_line",
    )

    # print(f"table_id: {table_id}, page_num: {page_num}")
    if table is not None:
        if table.get("data") is not None:
            [t_x0, t_top, t_x1, t_bottom] = table["data"].bbox
            prev_item = {"x0": t_x0, "x1": t_x1, "top": t_top, "bottom": t_bottom}
            # 算出每个item的间距（第一个是table，然后是最靠着table的text，依次外推）
            for index, item in enumerate(top_data[::-1]):
                gap = prev_item["top"] - item["data"]["bottom"]
                if gap <= GAP:
                    # print(table_id, 'top', gap)
                    result.append(item)
                else:
                    break
                prev_item = item["data"]
            result.reverse()
            # TODO 需要上溯的表一定是当前页的第一张表，id以_1结尾
            if table_id.split("_")[1] == "1":
                # 这里判断下，如果result数量很少，比如只有1个，而且是“单位：元”这种，就继续翻上一页，找到类似1、或者一、这种结束
                effective_count = len(result)
                for item in result:
                    if "单位" in item["data"]["text"] and "元" in item["data"]["text"]:
                        effective_count -= 1
                        break
                # print(f"有效topdesc统计 {table_id}: {effective_count}")

                if effective_count <= 2:
                    print(f"有效top_desc很少的table: {table_id}")
                    prev_page_num = page_num - 1
                    # 改：第一页无需上溯
                    if prev_page_num > 0:
                        prev_page = page_struct[prev_page_num]
                        prev_count = 0
                        for item in prev_page[::-1][1:]:
                            if item["type"] == "text_line":
                                result.insert(0, item)
                                prev_count += 1
                                if "、" in item["data"]["text"]:
                                    break
                                if prev_count > 3:
                                    break
                            else:
                                break

        return result


GAP = 30


def get_table_bottom_desc(table_id: str, page_struct: any):
    result = []
    page_num = parse_table_id(table_id)["page_num"]
    table = _find(page_struct[page_num], lambda item: item["id"] == table_id)
    bottom_data = _filter(
        get_bottom_table_data(table_id, page_struct)[:-1],
        lambda item: item["type"] == "text_line",
    )
    if table is not None:
        if table.get("data") is not None:
            [t_x0, t_top, t_x1, t_bottom] = table["data"].bbox
            prev_item = {"x0": t_x0, "x1": t_x1, "top": t_top, "bottom": t_bottom}

            for index, item in enumerate(bottom_data):
                gap = item["data"]["top"] - prev_item["bottom"]
                if gap <= GAP:
                    # print(table_id, 'bottom', gap, item['data']['text'])
                    result.append(item)
                else:
                    break
                prev_item = item["data"]

    return result


def get_table_desc(table_id: str, page_struct: any):
    top_desc = get_table_top_desc(table_id, page_struct)
    bottom_desc = get_table_bottom_desc(table_id, page_struct)

    top_desc_list = _map(top_desc, lambda item: item["data"]["text"])
    bottom_desc_list = _map(bottom_desc, lambda item: item["data"]["text"])

    return {"top": top_desc_list, "bottom": bottom_desc_list}


def gen_table_model(
    id: str, range: list[str], name: str = None, desc: dict[str, list[str]] = None
):
    return {
        "id": id,
        "range": range,
        "name": name,
        "desc": desc,
    }


def find_table_from_page_struct(table_id: str, page_struct: dict[int, list[dict]]):
    page_num = parse_table_id(table_id)["page_num"]
    page_data = page_struct[page_num]

    table = _find(page_data, lambda item: item["type"] == "table")
    return table


def gen_table_name(table_id: str, page_struct: any):
    # 获取去掉页眉的top数据
    top_data = _filter(
        get_top_table_data(table_id, page_struct)[1:],
        lambda item: item["type"] == "text_line",
    )
    for data in top_data[::-1]:
        text = data["data"]["text"].strip()
        if has_chinese_number(text):
            return text
    return None


def gen_table_content_model(id: str):
    return {"id": id, "data": {"fields": [], "rows": [], "origin": any}}


# 输出 1. base.json 2.table.json 3.财务报表.json
def parse_pdf(pdf_url, pdf_name):
    with pdfplumber.open(pdf_url) as pdf:
        table_json_url = f"{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{pdf_name}__table.json"
        content_json_url = f"{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{pdf_name}__content.json"

        # 如果table.json和content.json都存在，则不进行parse，直接return

        all_exists = is_exist(get_path(table_json_url)) and is_exist(
            get_path(content_json_url)
        )
        if all_exists:
            print(f"{pdf_name} 已经parse过了，不需要parse了!!")
            return
        print(f"{pdf_name} 开始parse!")
        prev_table = None
        prev_table_id = None
        prev_table_index = None
        prev_table_count = None

        page_struct = {}
        maybe_same_tables = {}
        # _pages = [pdf.pages[150], pdf.pages[151]]
        _pages = pdf.pages
        # _pages = pdf.pages[26:27]
        # _pages = pdf.pages
        table_id_list = []
        color_info = {
            "name": pdf_name,
            "line": {"count": 0, "non_stroking_color": {}, "stroking_color": {}},
            "rect": {"count": 0, "non_stroking_color": {}, "stroking_color": {}},
        }
        analysis_color = {
            "name": pdf_name,
            "leading_colors": [],
            "rect_color_empty": True,
            "line_small": True,
        }

        # 将color格式由dict改为逆序的二维数组[[color, count]]
        # dict : {color:count, ...}
        def get_color_list(color_dict: dict):
            arr: list[list] = []
            for color in color_dict:
                count = color_dict[color]
                arr.append([color, count])
            arr.sort(reverse=True, key=lambda item: (item[1]))
            return arr

        # 如果是相同元素的元组，返回元组第一个值
        # 如(0,0)返回0
        def format_color(color):
            if isinstance(color, list):
                color = tuple(color)
                if is_list_item_same(color):
                    return color[0]
            return color

        def color_distribution(obj):
            type = obj["object_type"]
            if type in ("rect", "line"):
                color_info[type]["count"] += 1
                # color_empty_count = 0
                for color_type in ["stroking_color", "non_stroking_color"]:
                    color = format_color(obj[color_type])
                    color_map = color_info[type][color_type]
                    if color_map.get(color) is None:
                        color_map[color] = 0
                    color_map[color] += 1

            return True

        def analysis_color_info(color_info):
            def is_color_empty(arr: list[list]):
                if len(arr) == 0:
                    return True
                elif len(arr) == 1 and arr[0][0] == None:
                    return True
                return False

            line_count = color_info["line"]["count"]
            rect_count = color_info["rect"]["count"]
            leading_colors = []

            line_non_color = color_info["line"]["non_stroking_color"]
            line_color = color_info["line"]["stroking_color"]

            rect_non_color = color_info["rect"]["non_stroking_color"]
            rect_color = color_info["rect"]["stroking_color"]

            # rect按道理是不应该有stroking_color的
            analysis_color["rect_color_empty"] = is_color_empty(rect_color)

            # line_count==0或者不到rect数量1/10，占据了(81+24)/120 = 87.5%的情况
            analysis_color["line_small"] = (
                line_count == 0 or rect_count / line_count >= 10
            )
            if analysis_color["line_small"]:
                leading_colors = []
                main_color = rect_non_color[0]
                leading_colors.append(main_color[0])
                if len(rect_non_color) >= 2:
                    second_color = rect_non_color[1]
                    if second_color[1] / main_color[1] >= 0.5:
                        leading_colors.append(second_color[0])
            # line比rect多 占据 12/120 = 10%的情况
            elif line_count > rect_count:
                leading_colors = []
                main_color = line_color[0]
                leading_colors.append(main_color[0])
            # rect比line稍微多一点 占据3/120 = 2.5%的情况
            else:
                leading_colors = []
                print("react比line稍微多一点：", line_color)
                main_color = line_color[0]
                leading_colors.append(main_color[0])

            analysis_color["leading_colors"] = leading_colors
            return analysis_color

        def keep_visible_lines(obj):
            if obj["object_type"] == "rect":
                non_color = format_color(obj["non_stroking_color"])
                leading_colors = analysis_color["leading_colors"]
                if non_color in leading_colors:
                    return True
                else:
                    return False
            return True

        def append_same_table(prev_table_id, table_id):
            # 获取map最后的key名
            last_key = get_dict_key_by_index(maybe_same_tables, -1)
            if last_key is None or prev_table_id not in maybe_same_tables[last_key]:
                if maybe_same_tables.get(prev_table_id) is None:
                    maybe_same_tables[prev_table_id] = [prev_table_id]
                maybe_same_tables[prev_table_id].append(table_id)
            else:
                maybe_same_tables[last_key].append(table_id)

        def get_common_row(rows):
            target_row = None
            for row in rows:
                # 第一个cell有文字，其他cell都是None
                first_cell = row.cells[0]
                other_cells = row.cells[1:]
                if not (
                    first_cell is not None
                    and _every(other_cells, lambda item: item is None)
                ):
                    target_row = row
                    break
            if target_row:
                return target_row
            return None

        # 全页面颜色扫描
        for page_index, page in enumerate(pdf.pages):
            print(f"color_scan --{pdf_name}  {page_index + 1}/{len(pdf.pages)}--", "\n")
            page = page.filter(color_distribution)
            page.find_tables()
        # 将color格式由dict改为逆序的二维数组[[color, count]]
        for type in ["line", "rect"]:
            for color_type in ["stroking_color", "non_stroking_color"]:
                color_dict = color_info[type][color_type]
                color_arr = get_color_list(color_dict)
                color_info[type][color_type] = color_arr
        print("color_info: ", color_info)
        analysis_color = analysis_color_info(color_info)
        # return color_info, analysis_color
        # json("/demo1.json", color_info)
        # json("/demo2.json", analysis_color)

        # 数据提取
        for page_index, page in enumerate(_pages):
            print(f"--{pdf_name}  {page_index + 1}/{len(_pages)}--", "\n")
            # Filter out hidden lines.
            page = page.filter(keep_visible_lines)
            tables = page.find_tables()
            # print(len(tables))
            # if len(tables):
            #     im = page.to_image()
            #     im.debug_tablefinder(tf={"vertical_strategy": 'lines', "horizontal_strategy": "lines"}).show()

            # continue

            # 提取页面的文字
            text_lines = page.extract_text_lines()

            # 确定页面里文字和表格关系
            if page_struct.get(page_index + 1) is None:
                page_struct[page_index + 1] = []
                current_page_struct = page_struct[page_index + 1]
            table_index = 0
            for text_line in text_lines:
                top = text_line["top"]
                bottom = text_line["bottom"]
                x0 = text_line["x0"]
                x1 = text_line["x1"]

                if table_index <= len(tables) - 1:
                    table = tables[table_index]
                    [t_x0, t_top, t_x1, t_bottom] = table.bbox
                    # 在表格上方
                    if bottom < t_top:
                        current_page_struct.append(page_obj("text_line", text_line))
                    # 在表格下方
                    elif top > t_bottom:
                        current_page_struct.append(
                            page_obj(
                                "table", table, gen_table_id(page_index, table_index)
                            )
                        )
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
                    prev_table_first_row = prev_table.rows[0]
                    prev_table_last_row = prev_table.rows[-1]
                    current_table_first_row = table.rows[0]
                    # if page_index + 1 == 115:
                    #     print("当前表第一行", table.rows[0].cells)
                    #     print("当前表第二行", table.rows[1].cells)

                    # 如果是相同表，一定得满足的条件是：（本表是本页第一张表，上表是上页最后一张表）
                    same_table_necessary_condition = (
                        table_index == 0 and prev_table_index + 1 == prev_table_count
                    )
                    has_append = False
                    # 如果符合必要条件，且当前表的top_desc中存在（续）做结尾的描述，则视为前表的续表
                    table_desc = get_table_desc(table_id, page_struct)
                    top_desc: list[str] = table_desc["top"]
                    for desc_item in top_desc:
                        if desc_item.strip().endswith("（续）"):
                            has_append = True
                            append_same_table(prev_table_id, table_id)
                            break

                    if (
                        same_table_necessary_condition
                        and not has_append
                        and len(
                            get_top_table_data(table_id, page_struct)
                            + get_bottom_table_data(prev_table_id, page_struct)
                        )
                        <= 2
                    ):
                        # 如果列数一致，那么就是同结构
                        # 如果不是同结构，就再判断下，是否是因为有【归纳行】（占一整行的文字做归纳）
                        prev_table_common_row = get_common_row(prev_table.rows)
                        current_table_common_row = get_common_row(table.rows)

                        if prev_table_common_row and current_table_common_row:
                            if is_cells_size_same(
                                prev_table_common_row.cells,
                                current_table_common_row.cells,
                            ):
                                append_same_table(prev_table_id, table_id)

                        # if is_cells_size_same(prev_table_first_row.cells, current_table_first_row.cells):
                        #     append_same_table(prev_table_id, table_id)

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
                all_tables.append(
                    gen_table_model(table_id, [table_id], desc=table_desc)
                )
            else:
                same_ids = maybe_same_tables.get(table_id)
                # 为空说明不是逻辑表的第一张表，所以不需要管，不为空说明是头表，直接加入all_tables即可
                if same_ids is not None:
                    # 获取table上面的text_lines，从而给出一个推测的name
                    # table_name = gen_table_name(table_id, page_struct)
                    table_top_desc = get_table_desc(table_id, page_struct)["top"]
                    table_bottom_desc = get_table_desc(same_ids[-1], page_struct)[
                        "bottom"
                    ]
                    table_desc = {"top": table_top_desc, "bottom": table_bottom_desc}
                    all_tables.append(
                        gen_table_model(table_id, same_ids, desc=table_desc)
                    )

        # 输出json
        json(table_json_url, all_tables)
        # page_struct中的table替换为table.extract的文本
        base_content = {}
        for page_num in page_struct:
            page_info_list = page_struct[page_num]
            base_content[page_num] = []
            for item in page_info_list:
                if item["type"] == "table":
                    item["data"] = _map(item["data"].extract(), lambda item: item)
                elif item["type"] == "text_line":
                    item["data"] = item["data"]["text"]
                base_content[page_num].append(list(item.values()))

                # item['data'] = item['data'].extract()
                # [type, id, data]
        json(content_json_url, base_content)

        # return page_struct


def get_color_statistic(pdf_url):
    result = {}

    def page_filter(obj):
        if obj["object_type"] == "rect":
            non_stroking_color = obj["non_stroking_color"]
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
    def __init__(self, thread_name, file_title_list, start_index, end_index):
        self.thread_name = thread_name
        self.file_title_list = file_title_list
        self.start_index = start_index
        self.end_index = end_index
        super().__init__()

    def run(self):
        arr = self.file_title_list[self.start_index : self.end_index + 1]
        arr_length = len(arr)
        for i, file_title in enumerate(arr):
            index = self.start_index + i
            pdf_url = get_path(f"{STATIC_ANNOUNCEMENTS_DIR}/{file_title}.pdf")
            table_json_url = get_path(
                f"{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{file_title}__table.json"
            )
            print(
                f"{self.thread_name}: {file_title}, 线程内进度：{i+1}/{arr_length}, 总任务第几个：{index + 1}"
            )
            exists = os.path.exists(table_json_url)
            if not exists:
                parse_pdf(pdf_url, pdf_name=file_title)


def parse_announcements(start_year, end_year):
    point_year = start_year
    or_str = ""
    while point_year <= end_year:
        suffix = " or " if point_year < end_year else ""
        or_str += f'title like "%{point_year}%"{suffix}'
        point_year += 1

    r = read_table(
        table_name="announcements",
        fields=["file_title"],
        result_type="dict",
        filter_str=f"""
                    where title not like "%英文%" and 
                    title not like "%取消%" and 
                    title not like "%摘要%" and 
                    title not like "%公告%" and
                    (
                        {or_str}
                    )
                """,
    )
    file_title_list = _map(r, lambda item: item["file_title"])
    middle = math.ceil(len(file_title_list) / 2)
    t1 = ParsePdfThread(
        thread_name="线程1",
        file_title_list=file_title_list,
        start_index=0,
        end_index=middle,
    )
    t1.start()
    t2 = ParsePdfThread(
        thread_name="线程2",
        file_title_list=file_title_list,
        start_index=middle + 1,
        end_index=len(file_title_list) - 1,
    )
    t2.start()

    print(f"多线程解析{start_year}-{end_year}年报完成")


def get_announcement_url(name):
    return get_path(f"{STATIC_ANNOUNCEMENTS_DIR}/{name}.pdf")


def gen_hbzcfzb(file_title, url):
    save_content_path = (
        get_path(STATIC_ANNOUNCEMENTS_PARSE_DIR) + "/" + file_title + "__content.json"
    )
    save_table_path = (
        get_path(STATIC_ANNOUNCEMENTS_PARSE_DIR) + "/" + file_title + "__table.json"
    )
    file_all_tables = json2(save_table_path)
    content = json2(save_content_path)

    hbzcfzb = None
    hbzcfzb_rows = []
    for t in file_all_tables:
        top_desc = t["desc"]["top"]
        for d in top_desc:
            if (
                d.endswith(f"{Financial_Statement.合并资产负债表.value}")
                or d.endswith(f"{Financial_Statement.合并及公司资产负债表.value}")
                or d.endswith(f"{Financial_Statement.资产负债表.value}")
            ):
                hbzcfzb = t
                break
        if hbzcfzb is not None:
            break

    if hbzcfzb:
        # print("嘿嘿", hbzcfzb)
        find_count = 0
        for p in content:
            p_values = content[p]
            for item in p_values:
                if item[0] == "table" and item[1] in hbzcfzb["range"]:
                    # print("奇怪", item[1])
                    find_count += 1
                    hbzcfzb_rows += item[2]
                if find_count == len(hbzcfzb["range"]):
                    break
            if find_count == len(hbzcfzb["range"]):
                break
        json2(f"{url}", hbzcfzb_rows)
        return True
    return False


def gen_hblrb(file_title, url):
    save_content_path = (
        get_path(STATIC_ANNOUNCEMENTS_PARSE_DIR) + "/" + file_title + "__content.json"
    )
    save_table_path = (
        get_path(STATIC_ANNOUNCEMENTS_PARSE_DIR) + "/" + file_title + "__table.json"
    )
    file_all_tables = json2(save_table_path)
    content = json2(save_content_path)

    hblrb = None
    hblrb_rows = []
    for t in file_all_tables:
        top_desc = t["desc"]["top"]
        for d in top_desc:
            # TODO 有些公司不存在“合并利润表”和“母公司利润表”，仅存在“利润表”
            if d.endswith(f"{Financial_Statement.合并利润表.value}") \
            or (f"、{Financial_Statement.合并利润表.value}" in d)\
            or d.endswith(f"{Financial_Statement.合并及公司利润表.value}")\
            or d.endswith(f"{Financial_Statement.利润表.value}"):
                hblrb = t
                break
        if hblrb is not None:
            break

    if hblrb:
        find_count = 0
        for p in content:
            p_values = content[p]
            for item in p_values:
                if item[0] == "table" and item[1] in hblrb["range"]:
                    find_count += 1
                    hblrb_rows += item[2]
                if find_count == len(hblrb["range"]):
                    break
            if find_count == len(hblrb["range"]):
                break
        json2(f"{url}", hblrb_rows)
        return True
    return False


def calculate_interest_bearing_liabilities(hbzcfzb_json):
    """
    计算有息负债
    """
    interest_items = [
        "短期借款",
        "交易性金融负债",
        "长期借款",
        "应付债券",
        "一年内到期的非流动负债",
        "一年内到期的融资租赁负债",
        "长期融资租赁负债",
    ]
    interest_bearing_liabilities_current_list = []
    interest_bearing_liabilities_current_list_new = []
    for row in hbzcfzb_json:
        result = _map(row, lambda item: large_num_format(item))
        if result[0] in interest_items:
            # 如果len(result)==3,则第二列为当年对应数据，如果len(result)==4,则第三列为当年对应数据
            if len(result) == 3:
                interest_bearing_liabilities_current_list.append(result[1])
            else:
                interest_bearing_liabilities_current_list.append(result[2])
    for item in interest_bearing_liabilities_current_list:
        item = 0 if (_is_empty(item) or item == "-") else item
        interest_bearing_liabilities_current_list_new.append(item)
    interest_bearing_liabilities_current_list_new = _map(
        interest_bearing_liabilities_current_list_new, lambda x: float(x)
    )
    interest_bearing_liabilities_current = sum(
        interest_bearing_liabilities_current_list_new
    )

    return interest_bearing_liabilities_current


def get_total_assets(hbzcfzb_json):
    fields = _map(hbzcfzb_json, lambda item: item[0])
    key_word = _filter(
        fields,
        lambda field: field.replace("\n", "")
        .replace("（", "")
        .replace("(", "")
        .replace("）", "")
        .replace(")", "")
        .replace("：", "")
        .replace(":", "")
        in ["负债和所有者权益或股东权益总计", "负债及股东权益合计", "负债和所有者权益总计", "负债和所有者权益或股东权益"],
    )
    # TODO 有些总资产由第一页末尾和第二页开头共同组成，解析为
    # [
    #     "负债和所有者权益（或股东",
    #     "",
    #     "4,536,412,952.00",
    #     "4,480,824,564.34"
    # ],
    # [
    #     "权益）总计",
    #     "",
    #     "",
    #     ""
    # ]
    for row in hbzcfzb_json:
        if row[0] in key_word:
            if len(row) == 3:
                row[1] = (
                    0.01
                    if (_is_empty(row[1]) or row[1] == "-")
                    else float(large_num_format(row[1]))
                )
                return row[1]
            else:
                row[2] = (
                    0.01
                    if (_is_empty(row[2]) or row[2] == "-")
                    else float(large_num_format(row[2]))
                )
                return row[2]


def get_operating_revenue(hblrb_json):
    """
    获取合并利润表中的营业收入及增长率
    """
    fields = _map(hblrb_json, lambda item: item[0])
    key_word = _filter(
        fields,
        lambda field: field.replace("\n", "")
        .replace("（", "")
        .replace("(", "")
        .replace("）", "")
        .replace(")", "")
        .replace("：", "")
        .replace(":", "")
        .replace("、", "")
        in ["其中营业收入", "一营业收入"],
    )
    current_operating_revenue_list = []
    last_operating_revenue_list = []
    for row in hblrb_json:
        if row[0] in key_word:
            if len(row) == 3:
                row[1] = (
                    0.01
                    if (_is_empty(row[1]) or row[1] == "-")
                    else float(large_num_format(row[1]))
                )
                row[2] = (
                    0.01
                    if (_is_empty(row[2]) or row[2] == "-")
                    else float(large_num_format(row[2]))
                )
                current_operating_revenue_list.append(row[1])
                last_operating_revenue_list.append(row[2])
            else:
                row[2] = (
                    0.01
                    if (_is_empty(row[2]) or row[2] == "-")
                    else float(large_num_format(row[2]))
                )
                row[3] = (
                    0.01
                    if (_is_empty(row[3]) or row[3] == "-")
                    else float(large_num_format(row[3]))
                )
                current_operating_revenue_list.append(row[2])
                last_operating_revenue_list.append(row[3])
    current_operating_revenue = sum(current_operating_revenue_list)
    last_operating_revenue = sum(last_operating_revenue_list)
    try:
        growth_rate = (current_operating_revenue - last_operating_revenue)/last_operating_revenue*100
        return current_operating_revenue, growth_rate  # 返回当期营业收入和营业收入增长率
    except:
        print("无法计算营业收入增长率")
        

def caculate_interest_bearing_liabilities_rate(
    interest_bearing_liabilities, total_assets
):
    """
    计算有息负债占比
    """
    interest_bearing_liabilities_rate = interest_bearing_liabilities / total_assets
    return interest_bearing_liabilities_rate * 100


def get_accounts_receivable(hbzcfzb_json):
    """
    计算应收款及应收增长率
    """
    # 应收 = 资产负债表所有带“应收”两个字的科目数字总和-银行承兑汇票金额
    # 1.筛选出合并资产负债表中包含“应收”关键字的字段值，计算其之和
    fields = _map(hbzcfzb_json, lambda item: item[0])
    key_word = _filter(fields, lambda field: "应收" in field)
    # 2.通过子表查找银行承兑汇票金额： 由于不是每个公司都有此项目（比例较小），可暂时放宽条件，仅设定默认值，后续再根据测试结果完善该方法
    amount_of_bankers_acceptance = 0
    # 3.计算当期应收及应收增长率
    current_accounts_receivable_list = []
    last_accounts_receivable_list = []
    for row in hbzcfzb_json:
        if row[0] in key_word:
            if len(row) == 3:
                row[1] = (
                    0
                    if (_is_empty(row[1]) or row[1] == "-")
                    else float(large_num_format(row[1]))
                )
                row[2] = (
                    0
                    if (_is_empty(row[2]) or row[2] == "-")
                    else float(large_num_format(row[2]))
                )
                current_accounts_receivable_list.append(row[1])
                last_accounts_receivable_list.append(row[2])
            else:
                row[2] = (
                    0
                    if (_is_empty(row[2]) or row[2] == "-")
                    else float(large_num_format(row[2]))
                )
                row[3] = (
                    0
                    if (_is_empty(row[3]) or row[3] == "-")
                    else float(large_num_format(row[3]))
                )
                current_accounts_receivable_list.append(row[2])
                last_accounts_receivable_list.append(row[3])
    current_accounts_receivable = sum(current_accounts_receivable_list)
    last_accounts_receivable = sum(last_accounts_receivable_list)
    try:
        growth_rate = (
            (current_accounts_receivable - last_accounts_receivable)
            / last_accounts_receivable
            * 100
        )
        return current_accounts_receivable, growth_rate
    except:
        print("无法计算应收款增长率")

def propotion_of_accounts_receivable(hbzcfzb_json):
    """
    计算应收/总资产比例
    """
    # 应收 = 资产负债表所有带“应收”两个字的科目数字总和-银行承兑汇票金额
    # 1.筛选出合并资产负债表中包含“应收”关键字的字段值，计算其之和
    fields = _map(hbzcfzb_json, lambda item: item[0])
    key_word = _filter(fields, lambda field: "应收" in field)
    # 2.通过子表查找银行承兑汇票金额： 由于不是每个公司都有此项目（比例较小），可暂时放宽条件，仅设定默认值，后续再根据测试结果完善该方法
    amount_of_bankers_acceptance = 0
    # 3.计算应收/总资产比例
    accounts_receivable_list = []
    for row in hbzcfzb_json:
        if row[0] in key_word:
            if len(row) == 3:
                row[1] = (
                    0
                    if (_is_empty(row[1]) or row[1] == "-")
                    else float(large_num_format(row[1]))
                )
                accounts_receivable_list.append(row[1])
            else:
                row[2] = (
                    0
                    if (_is_empty(row[2]) or row[2] == "-")
                    else float(large_num_format(row[2]))
                )
                accounts_receivable_list.append(row[2])
        accounts_receivable = sum(accounts_receivable_list)
    total_assets = get_total_assets(hbzcfzb_json)
    propotion_of_accounts_receivable = accounts_receivable / total_assets
    return propotion_of_accounts_receivable * 100

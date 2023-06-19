import os
import time

import requests

from db.index import clear_table, insert_table, get_total, delete_rows, read_table
from model.index import describe_json
from service.index import get_current_d_tables
from utils.index import json, _filter, date2str, is_iterable, _map, get_path, _is_empty
from datetime import date

STATIC_DIR = '/static/financial_statement'

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
        json(f'{STATIC_DIR}/juchao_all_stocks.json', response.json())
        return response.json()['stockList']
    return json(f'{STATIC_DIR}/juchao_all_stocks.json')['stockList']


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


def download_announcement(url: str, title: str, skip_if_exist: bool = True):
    save_path = get_path(STATIC_DIR) + "/" + title + '.pdf'
    # print('save_path', save_path)
    if skip_if_exist:
        # 判断下 title.pdf是否已经存在于静态文件夹，如果已经存在就直接return，不做操作
        is_exist = os.path.exists(save_path)
        if is_exist:
            print(f"{title}.pdf 已存在，不会重复下载 ")
            return is_exist
    try:
        response = requests.get(url, headers=JU_CHAO_HEADERS)
    except requests.exceptions.ConnectionError:
        print('请求pdf超负荷了，休息15s后将继续请求..')
        time.sleep(15)
        response = requests.get(url, headers=JU_CHAO_HEADERS)
    # wb指的是writebyte,以二进制形式写入
    open(save_path, 'wb').write(response.content)
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


def get_year_announcements(year):
    result = read_table(table_name="announcements",
                        filter_str=f"where title like '%{year}%' and title not like '%英文%'  and title not like "
                                   f"'%（已取消）%' and title not like '%摘要%'",
                        result_type="dict")
    return result

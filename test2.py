import threading
from db.index import read_table
from service.report import (
    STATIC_ANNOUNCEMENTS_DIR,
    STATIC_ANNOUNCEMENTS_PARSE_DIR,
    get_color_statistic,
    parse_pdf,
)
from utils.index import _map, concurrency, get_path, is_iterable
import os
from db.index import read_table
from service.report import STATIC_ANNOUNCEMENTS_DIR, get_color_statistic
from utils.index import get_path, json


def json_format_color_result(result):
    new_result = result.copy()
    for key in result:
        if is_iterable(key):
            value = result[key]
            new_result.pop(key)
            new_result[f"{key}"] = value
    return new_result
#  and title like '%2022%'
# json_format_color_result(get_color_statistic(pdf_url))
r = read_table(
    table_name="announcements",
    fields=["file_title"],
    result_type="dict",
    filter_str="where title not like '%英文%' and title not like '%取消%' and title not like '%摘要%' and title like '%2022%' ORDER BY RAND() LIMIT 3",
)

file_title_list = _map(r, lambda item: item["file_title"])


result = [None]*len(file_title_list)
def scan_color(file_title_list, start_index, end_index):
    arr = file_title_list[start_index:end_index+1]
    for i, file_title in enumerate(arr):
        index = start_index + i
        print(f"{threading.current_thread().name}: {file_title}")
        pdf_url = get_path(f"{STATIC_ANNOUNCEMENTS_DIR}/{file_title}.pdf")
        scan_result = parse_pdf(pdf_url, file_title)
        scan_result['line']['color'] = json_format_color_result(scan_result['line']['color'])
        scan_result['rect']['color'] = json_format_color_result(scan_result['rect']['color'])
        result[index] = scan_result

concurrency(
    run=scan_color,
    arr=file_title_list,
    count = 3
)
# # print("result: ",result)
json("/scan_color1.json", result)




# pdf_url = get_path(f'{STATIC_ANNOUNCEMENTS_DIR}/{file_title}.pdf')
# table_json_url = get_path(f'{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{file_title}__table.json')
# print(pdf_url, f" {index + 1}/{len(r)}")
# exists = os.path.exists(table_json_url)
# if not exists:
#     parse_pdf(pdf_url, pdf_name=file_title)


# url = "./static/announcements/301123__奕东电子__2022年年度报告__1216551617.pdf"
# get_color_statistic(url)


# pdf_name = "301123__奕东电子__2022年年度报告__1216551617"
# pdf_name = "000002__万科A__2007年年度报告__38155886"
# pdf_name = "000002__万科A__2022年年度报告__1216273938"
# pdf_name = "000623__吉林敖东__2022年年度报告__1216455868"
# pdf_name = "000625__长安汽车__2022年年度报告__1216442229"
# pdf_name = "000068__华控赛格__2022年年度报告__1216701006"
# pdf_name = "hgcy"
# pdf_url = get_path("reports/hgcy.pdf")
# pdf_url = get_path(f'{STATIC_ANNOUNCEMENTS_DIR}/{pdf_name}.pdf')
# a = get_color_statistic(pdf_url)
# print(a)
# parse_pdf(pdf_url, pdf_name=pdf_name)





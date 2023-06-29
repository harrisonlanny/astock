from db.index import read_table
from service.report import STATIC_ANNOUNCEMENTS_DIR,STATIC_ANNOUNCEMENTS_PARSE_DIR, get_color_statistic, parse_pdf
from utils.index import get_path
import os

urls = []
r = read_table(table_name="announcements",
               fields=["file_title"],
               result_type="dict",
               filter_str="where title not like '%英文%' and title not like '%取消%' and title not like '%摘要%' limit 2")

for index, rs in enumerate(r):
    file_title = rs['file_title']
    pdf_url = get_path(f'{STATIC_ANNOUNCEMENTS_DIR}/{file_title}.pdf')
    table_json_url = get_path(f'{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{file_title}__table.json')
    print(pdf_url, f" {index + 1}/{len(r)}")
    exists = os.path.exists(table_json_url)
    if not exists:
        parse_pdf(pdf_url, pdf_name=file_title)


# url = "./static/announcements/000001__平安银行__2017年年度报告__1204477157.pdf"
# get_color_statistic(url)
# pdf_url = get_path(f'{STATIC_ANNOUNCEMENTS_DIR}/000001__平安银行__2021年年度报告__1212533413.pdf')
# parse_pdf(pdf_url, pdf_name='000001__平安银行__2021年年度报告__1212533413.pdf')
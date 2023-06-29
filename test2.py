from db.index import read_table
from service.report import STATIC_ANNOUNCEMENTS_DIR, get_color_statistic, parse_pdf
from utils.index import get_path
urls = []
r = read_table(table_name="announcements",
               fields=["file_title"],
               result_type="dict",
               filter_str="where title not like '%英文%' and title not like '%取消%' and title not like '%摘要%' limit 2")

for rs in r:
    file_title = rs['file_title']
    pdf_url = get_path(f'{STATIC_ANNOUNCEMENTS_DIR}/{file_title}.pdf')
    print(pdf_url)
    parse_pdf(pdf_url, pdf_name=file_title)

print(isinstance(1, int))

# url = "./static/announcements/000001__平安银行__2017年年度报告__1204477157.pdf"
# get_color_statistic(url)
# pdf_url = get_path(f'{STATIC_ANNOUNCEMENTS_DIR}/000001__平安银行__2021年年度报告__1212533413.pdf')
# parse_pdf(pdf_url, pdf_name='000001__平安银行__2021年年度报告__1212533413.pdf')
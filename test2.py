from db.index import read_table
from service.report import STATIC_DIR

urls = []
r = read_table(table_name="announcements",
               fields=["file_title"],
               filter_str="where title not like '%英文%' and title not like '%取消%' and title not like '%摘要%' limit 20")
for rs in r:
    pdf_url = f'.{STATIC_DIR}/{rs[0]}.pdf'

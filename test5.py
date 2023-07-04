from service.report import get_announcement_url, parse_pdf
from utils.index import get_path

name = "000534__万泽股份__2022年年度报告__1215991366"
pdf_url = get_announcement_url(name)
parse_pdf(pdf_url, name)
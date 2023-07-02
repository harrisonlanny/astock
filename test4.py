
from service.report import (
    STATIC_ANNOUNCEMENTS_DIR,
    STATIC_ANNOUNCEMENTS_PARSE_DIR,
    get_color_statistic,
    parse_pdf,
)
from utils.index import get_path, json

name = "300700__岱勒新材__2022年年度报告__1216050813"
path = get_path(f"{STATIC_ANNOUNCEMENTS_DIR}/{name}.pdf")

parse_pdf(path, name)
# json("/color_info.json", color_info)
# json("/analysis_color_info.json", analysis_info)

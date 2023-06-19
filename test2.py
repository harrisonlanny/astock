# from service.index import get_current_d_tables
# from utils.index import _map
#
# result = _map(get_current_d_tables(), lambda item: item[2:])
# print(result)
# from service.report import download_announcement
# from utils.index import json
#
# data = json('/static/financial_statement/announcements.json')
# announcements = data['688126']['announcements']
#
# for announcement in announcements:
#     url = announcement['url']
#     title = announcement['file_title']
#     download_announcement(url, title)
# from db.index import update_table_fields
#
from db.index import update_table_fields, read_table

# update_table_fields('announcements', update_field_defines={
#     # "file_title": "file_title VARCHAR(120) NOT NULL PRIMARY KEY",
#     "title": "title VARCHAR(80)",
# })
# from db.index import delete_table, create_table
# from model.index import describe_json
#
# delete_table('announcements')
# describe = describe_json('announcements')
# create_table('announcements', describe.safe_columns)
import os.path

from service.report import refresh_table_announcements

# from service.report import refresh_table_announcements
#
# refresh_table_announcements([
#     '000003'
# ])

# save_path = "/Users/sunzheng/PycharmProjects/astock/static/financial_statement/000003__PT金田A__PT金田Ａ2001年年度报告（英文版）.pdf"
# print('exists', os.path.exists(save_path))
# refresh_table_announcements([
#     '600256'
# ])
result = read_table(table_name="announcements", filter_str="where title like '%2005%' and title not like '%英文%'  and title not like '%（已取消）%' and title not like '%摘要%'",result_type="dict")
print(len(result))
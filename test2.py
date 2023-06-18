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
# update_table_fields('announcements', update_field_defines={
#     "file_title": ""
# })
from db.index import delete_table, create_table
from model.index import describe_json

delete_table('announcements')
describe = describe_json('announcements')
create_table('announcements', describe.safe_columns)

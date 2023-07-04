
import os
from db.index import read_table
from model.model import TableModel
from service.report import STATIC_ANNOUNCEMENTS_HBZCFZB_DIR, STATIC_ANNOUNCEMENTS_PARSE_DIR, Financial_Statement, find_table_from_page_struct, gen_hbzcfzb, get_announcement_url, parse_pdf
from utils.index import _map, get_path, is_exist, json, json2, large_num_format

# name = "000534__万泽股份__2022年年度报告__1215991366"
# pdf_url = get_announcement_url(name)
# parse_pdf(pdf_url, name)

# 这里的20条随机，是当成所有的年报pdf来看的
# 当parse_pdf结束后，只应该有content.json 和table.json
# 后面所有的分析与提表，都基于上面两个json
# 

# r = read_table(
#     table_name="announcements",
#     fields=["file_title"],
#     result_type="dict",
#     filter_str="where title not like '%英文%' and title not like '%取消%' and title not like '%摘要%' and title like '%2022%' ORDER BY RAND() LIMIT 20",
# )
# file_title_list = _map(r, lambda item: item["file_title"])

# json2("/mock.json", file_title_list)

# file_title_list = _map(r, lambda item: item["file_title"])
# file_title_list = ["002047__宝鹰股份__2022年年度报告__1216563514"]
# calculate_interest_bearing_liabilities()


file_title_list = [
    "600481__双良节能__双良节能系统股份有限公司2022年年度报告__1216560014",
    "003005__竞业达__2022年年度报告__1216617405",
    # "301098__金埔园林__2022年年度报告__1216558940",
    # "002500__山西证券__2022年年度报告__1216656433",
    # "002282__博深股份__2022年年度报告__1216648090",
    # "603985__恒润股份__江阴市恒润重工股份有限公司2022年年度报告__1216419056",
    # "002581__未名医药__2022年度报告（更正后）__1217066287",
    # "688227__品高股份__2022年年度报告__1216711726",
    # "601788__光大证券__光大证券股份有限公司2022年年度报告__1216279948",
    # "003040__楚天龙__2022年年度报告__1216275830",
    # "300668__杰恩设计__2022年年度报告__1216517939",
    # "600278__东方创业__东方创业2022年度报告__1216701140",
    # "831305__海希通讯__2022年年度报告（更正后）__1216861207",
    # "300097__智云股份__2022年年度报告（更新后）__1216921699",
    # "603167__渤海轮渡__渤海轮渡集团股份有限公司2022年年度报告__1216441667",
    # "688510__航亚科技__无锡航亚科技股份有限公司2022年年度报告__1216582882",
    # "688288__鸿泉物联__鸿泉物联：2022年年度报告__1216687349",
    # "600500__中化国际__中化国际2022年年度报告__1216663256",
    # "603003__龙宇股份__龙宇股份2022年年度报告__1216645157",
    # "688538__和辉光电__上海和辉光电股份有限公司2022年年度报告__1216623370"
]

# 1. 遍历所有的file_title_list,并parse_pdf (已经parse过就不会再parse!!)
for file_title in file_title_list:
    file_url = get_announcement_url(file_title)
    parse_pdf(file_url, file_title)

# 2. 遍历所有的file_title_list，根据table.json和content.json来生成合并资产负债表
error_file_title_list = []
for file_title in file_title_list:
        # 缺乏必要的json，而无法合成资产负债表等明细表的file_title_list
        hbzcfzb_json_url = f"{STATIC_ANNOUNCEMENTS_HBZCFZB_DIR}/{file_title}__{Financial_Statement.合并资产负债表.value}.json"
        table_json_url = f"{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{file_title}__table.json"
        content_json_url = f"{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{file_title}__content.json"
       
        # 1. 检查是否存在合并资产负债表，如果不存在，才进行合成
        if is_exist(get_path(hbzcfzb_json_url)):
            continue
        # 2. 如果需要合成，检查必要的合成元素 table.json和content.json是否存在，如果存在 才进行合成，如果不存在
        # 可以收集异常的数据，并返回
        all_exists = is_exist(get_path(table_json_url)) and is_exist(get_path(content_json_url))
        if not all_exists:
            error_file_title_list.append({
                 "file_title": file_title,
                 "reason": "缺少table.json或content.json"
            })
            continue
        # 3. 进行合成
        gen_success = gen_hbzcfzb(file_title, hbzcfzb_json_url)
        if not gen_success:
            error_file_title_list.append({
                 "file_title": file_title,
                 "reason": "table.json中没找到合并资产负债表"
            })

print("合并有问题的file_title_list: ", error_file_title_list)
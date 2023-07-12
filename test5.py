from db.index import read_table
from service.report import STATIC_ANNOUNCEMENTS_HBLRB_DIR, Financial_Statement, get_operating_revenue
from strategy.announcements.announcements import filter_by_interest_bearing_liabilities, filter_by_proportion_of_accounts_receivable, generate_hblrb
# from strategy.announcements.announcements import filter_by_interest_bearing_liabilities
from utils.index import _map, get_path, json

# file_title_list = [
#     "600481__双良节能__双良节能系统股份有限公司2022年年度报告__1216560014",
#     "003005__竞业达__2022年年度报告__1216617405",
#     "301098__金埔园林__2022年年度报告__1216558940",
#     "002500__山西证券__2022年年度报告__1216656433",
#     "002282__博深股份__2022年年度报告__1216648090",
#     "603985__恒润股份__江阴市恒润重工股份有限公司2022年年度报告__1216419056",
#     "002581__未名医药__2022年度报告（更正后）__1217066287",
#     "688227__品高股份__2022年年度报告__1216711726",
#     "601788__光大证券__光大证券股份有限公司2022年年度报告__1216279948",
#     "003040__楚天龙__2022年年度报告__1216275830",
#     "300668__杰恩设计__2022年年度报告__1216517939",
#     "600278__东方创业__东方创业2022年度报告__1216701140",
#     "831305__海希通讯__2022年年度报告（更正后）__1216861207",
#     "300097__智云股份__2022年年度报告（更新后）__1216921699",
#     "603167__渤海轮渡__渤海轮渡集团股份有限公司2022年年度报告__1216441667",
#     "688510__航亚科技__无锡航亚科技股份有限公司2022年年度报告__1216582882",
#     "688288__鸿泉物联__鸿泉物联：2022年年度报告__1216687349",
#     "600500__中化国际__中化国际2022年年度报告__1216663256",
#     "603003__龙宇股份__龙宇股份2022年年度报告__1216645157"
# ]
# r = read_table(
#     table_name="announcements",
#     fields=["file_title"],
#     result_type="dict",
#     filter_str="where title not like '%英文%' and title not like '%取消%' and title not like '%摘要%' and title like '%2022%' ORDER BY RAND() LIMIT 100",
# )
# file_title_list = _map(r, lambda item: item["file_title"])
file_title_list = [
    "605056__咸亨国际__咸亨国际：2022年年度报告__1216478420",
    "600123__兰花科创__兰花科创2022年度报告全文__1216554351", # 应收款项不在合并资产负债表中，而在“1. 资产及负债状况 ”子表中
    "600392__盛和资源__盛和资源2022年年度报告__1216689975", # 合并资产负债表边框有开口导致无法解析出总资产
    # "836433__大唐药业__2022年年度报告__1216619159", # 边框颜色导致无法识别合并资产负债表、合并利润表
    "688778__厦钨新能__厦门厦钨新能源材料股份有限公司2022年年度报告__1216520043", # 134-1，135-1，136-1未识别为同一张表
    "688121__卓然股份__2022年年度报告（修订版）__1217053279" # 129-1,130-1,131-1未识别为同一张表

]

# filter_by_interest_bearing_liabilities(file_title_list)
# filter_by_proportion_of_accounts_receivable(file_title_list)

for file_title in file_title_list:
    pdf_url = f"{STATIC_ANNOUNCEMENTS_HBLRB_DIR}/{file_title}__{Financial_Statement.合并利润表.value}.json"
    r = json(pdf_url)
    print(f"{file_title}的营业收入为：{get_operating_revenue(r)}")
# generate_hblrb(file_title_list)

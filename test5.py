import statistics
from service.config import (
    STATIC_ANNOUNCEMENTS_HBLRB_DIR,
    STATIC_ANNOUNCEMENTS_HBZCFZB_DIR,
    Financial_Statement,
)
from db.index import read_table
from service.report import (
    gen_hbzcfzb,
    generate_announcement,
    get_accounts_receivable,
    get_cash_and_cash_equivalents,
    get_companies_in_the_same_industry,
    get_industry,
    get_monetary_fund,
    get_operating_revenue,
    parse_pdf_to_content_json,
    receivable_balance_propotion_of_monthly_average_operating_income,
)
from strategy.announcements.announcements import (
    filter_by_cash_to_debt_ratio,
    filter_by_increase_in_accounts_receivable,
    filter_by_interest_bearing_liabilities,
    filter_by_monetary_funds,
    filter_by_proportion_of_accounts_receivable,
    filter_by_receivable_balance,
)

# from strategy.announcements.announcements import filter_by_interest_bearing_liabilities
from utils.index import (
    _filter,
    _is_empty,
    _map,
    get_median,
    get_path,
    json,
    large_num_format,
)

# file_title_list = [
# "000627__天茂集团__2020年年度报告__1209866445",
# "000557__西部创业__2022年年度报告__1216547631",
# "601816__京沪高铁__京沪高铁2022年年度报告__1216707286",
# "600481__双良节能__双良节能系统股份有限公司2022年年度报告__1216560014",
# "003005__竞业达__2022年年度报告__1216617405",
# "301098__金埔园林__2022年年度报告__1216558940",
# "002500__山西证券__2022年年度报告__1216656433",
# "000001__平安银行__2007年年度报告__38090685",
# "002416__爱施德__2022年年度报告__1216442145",
# "002162__悦心健康__2022年年度报告__1216144736",
# "002282__博深股份__2022年年度报告__1216648090",
# "603985__恒润股份__江阴市恒润重工股份有限公司2022年年度报告__1216419056",
# "002581__未名医药__2022年度报告（更正后）__1217066287",
# "688227__品高股份__2022年年度报告__1216711726",
# "601788__光大证券__光大证券股份有限公司2022年年度报告__1216279948",
# "003040__楚天龙__2022年年度报告__1216275830",
# "300668__杰恩设计__2022年年度报告__1216517939",
#     "600278__东方创业__东方创业2022年度报告__1216701140",
#     "831305__海希通讯__2022年年度报告（更正后）__1216861207",
#     "300097__智云股份__2022年年度报告（更新后）__1216921699",
#     "603167__渤海轮渡__渤海轮渡集团股份有限公司2022年年度报告__1216441667",
#     "688510__航亚科技__无锡航亚科技股份有限公司2022年年度报告__1216582882",
#     "688288__鸿泉物联__鸿泉物联：2022年年度报告__1216687349",
#     "600500__中化国际__中化国际2022年年度报告__1216663256",
#     "603003__龙宇股份__龙宇股份2022年年度报告__1216645157",
#     "000636__风华高科__2022年年度报告__1216238318"
#  ]
# r = read_table(
#     table_name="announcements",
#     fields=["file_title"],
#     result_type="dict",
#     filter_str="where title not like '%英文%' and title not like '%取消%' and title not like '%摘要%' and title like '%2022%' ORDER BY RAND() LIMIT 200",
# )
# file_title_list = _map(r, lambda item: item["file_title"])

# file_title_list = [
# #     "601328__交通银行__交通银行2022年度报告__1216276220",  # 表格无边框
#     "601881__中国银河__中国银河：2022年年度报告__1216263010",  # 表格无边框
# #     "688265__南模生物__2022年年度报告__1216671531"# 无利润表
# ]

# file_title_list = [
# # "605056__咸亨国际__咸亨国际：2022年年度报告__1216478420",
# # "600123__兰花科创__兰花科创2022年度报告全文__1216554351", # 应收款项不在合并资产负债表中，而在“1. 资产及负债状况 ”子表中
# # "600392__盛和资源__盛和资源2022年年度报告__1216689975", # 合并资产负债表边框有开口导致无法解析出总资产
# # "836433__大唐药业__2022年年度报告__1216619159", # 边框颜色导致无法识别合并资产负债表、合并利润表
# # "688778__厦钨新能__厦门厦钨新能源材料股份有限公司2022年年度报告__1216520043", # 134-1，135-1，136-1未识别为同一张表
# # "688121__卓然股份__2022年年度报告（修订版）__1217053279" # 129-1,130-1,131-1未识别为同一张表
# ]

# filter_by_interest_bearing_liabilities(file_title_list)
# filter_by_proportion_of_accounts_receivable(file_title_list)

# generate_hbzcfzb([
#     '000001__平安银行__2007年年度报告__38090685'
#     # '605056__咸亨国际__咸亨国际：2022年年度报告__1216478420'
# ], False)
# generate_hblrb(file_title_list, False)
# filter_by_increase_in_accounts_receivable(file_title_list)


# for file_title in file_title_list:
#     # get_accounts_receivable(file_title)
#     get_operating_revenue(file_title)
# get_companies_in_the_same_industry("000001__平安银行__2006年年度报告__21676577",["银行"])


# filter_by_receivable_balance(file_title_list)
# filter_by_monetary_funds(file_title_list)
# generate_hbzcfzb(file_title_list, use_cache=False)
# generate_xjjxjdjw(file_title_list)
# for file_title in file_title_list:
#     receivable_balance_propotion_of_monthly_average_operating_income(file_title)

# filter_by_cash_to_debt_ratio(file_title_list)
# filter_by_monetary_funds(file_title_list)
# print("file_title_list:", file_title_list)
# error = [
#     {"file_title": "002666__德联集团__2022年年度报告__1216689819", "reason": "合并资产负债表生成失败"},
#     {"file_title": "301129__瑞纳智能__2022年年度报告__1216435406", "reason": "合并资产负债表生成失败"},
#     {
#         "file_title": "600000__浦发银行__上海浦东发展银行股份有限公司2022年年度报告（全文）__1216456201",
#         "reason": "合并资产负债表生成失败",
#     },
#     {"file_title": "300495__*ST美尚__2022年年度报告__1216628139", "reason": "合并资产负债表生成失败"},
#     {"file_title": "300402__宝色股份__2022年年度报告__1216591911", "reason": "合并资产负债表生成失败"},
#     {"file_title": "300058__蓝色光标__2022年年度报告__1216474890", "reason": "合并资产负债表生成失败"},
#     {"file_title": "300722__新余国科__2022年年度报告__1216577959", "reason": "合并资产负债表生成失败"},
#     {"file_title": "001289__龙源电力__2022年年度报告__1216261533", "reason": "合并资产负债表生成失败"},
#     {"file_title": "002519__银河电子__2022年年度报告__1216255304", "reason": "合并资产负债表生成失败"},
#     {"file_title": "300973__立高食品__2022年年度报告__1216647090", "reason": "合并资产负债表生成失败"},
#     {"file_title": "300098__高新兴__2022年年度报告__1216415432", "reason": "合并资产负债表生成失败"},
#     {"file_title": "300551__古鳌科技__2022年年度报告__1216649058", "reason": "合并资产负债表生成失败"},
#     {"file_title": "000078__海王生物__2022年年度报告__1216662203", "reason": "合并资产负债表生成失败"},
#     {"file_title": "301153__中科江南__2022年年度报告__1216272240", "reason": "合并资产负债表生成失败"},
#     {"file_title": "002828__贝肯能源__2022年年度报告__1216661594", "reason": "合并资产负债表生成失败"},
#     {"file_title": "002809__红墙股份__2022年年度报告__1216700022", "reason": "合并资产负债表生成失败"},
# ]
# error = _map(error, lambda item: item["file_title"])
file_title_list = ["601288__农业银行__农业银行2022年度报告__1216275777"]
# generate_hbzcfzb(file_title_list, use_cache=False)

# test_error_result = [
#     {
#         "file_title": "601211__国泰君安__国泰君安证券股份有限公司2022年年度报告__1216263383",
#         "reason": "合并资产负债表生成失败",# pdf格式问题
#     },
#     {"file_title": "000783__长江证券__2022年年度报告__1216691305", "reason": "合并资产负债表生成失败"},# 无合并资产负债表
#     {'file_title': '601860__紫金银行__江苏紫金农村商业银行股份有限公司2022年年度报告__1216707448', 'reason': '合并资产负债表生成失败'} # 无合并资产负债表
# ]

# parse_pdf_to_content_json()
generate_announcement(
    announcement_type=Financial_Statement.合并资产负债表,
    file_title_list=file_title_list,
    gen_table=gen_hbzcfzb,
    use_cache=False,
    consider_table=False,
)

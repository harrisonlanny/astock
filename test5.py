import statistics
import threading
from time import sleep
from service.config import (
    STATIC_ANNOUNCEMENTS_HBLRB_DIR,
    STATIC_ANNOUNCEMENTS_HBZCFZB_DIR,
    Financial_Statement,
)
from db.index import read_table
from service.report import (
    find_standard_unqualified_opinions,
    gen_cash_equivalents,
    gen_hblrb,
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
    filter_by_standard_unqualified_opinions,
)

# from strategy.announcements.announcements import filter_by_interest_bearing_liabilities
from utils.index import (
    _filter,
    _find_index,
    _is_empty,
    _map,
    concurrency,
    concurrency2,
    get_median,
    get_path,
    json,
    large_num_format,
    supplementing_rows_by_max_length,
)

file_title_list = [
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
 ]
r = read_table(
    table_name="announcements",
    fields=["file_title"],
    result_type="dict",
    filter_str="where title not like '%英文%' and title not like '%取消%' and title not like '%摘要%' and title not like '%公告%' and title not like '%修订前%' and title like '%2021%'",
)
file_title_list = _map(r, lambda item: item["file_title"]) # ORDER BY RAND() LIMIT 10

# file_title_list = [
#     "601328__交通银行__交通银行2022年度报告__1216276220",  # 表格无边框
#     "601881__中国银河__中国银河：2022年年度报告__1216263010",  # 表格无边框
#     "688265__南模生物__2022年年度报告__1216671531",# 无利润表
#     "600050__中国联通__中国联合网络通信股份有限公司2022年年度报告全文__1216074125",
#     "601512__中新集团__中新集团2022年年度报告__1216494929",
#     "603176__汇通集团__汇通集团2022年年度报告__1216589243",
#     "688018__乐鑫科技__乐鑫科技2022年年度报告__1216157066",
#     "600211__西藏药业__西藏药业2022年年度报告全文__1216093815",
#     "603191__望变电气__2022年年度报告__1216245898",
#     "000058__深赛格__2022年年度报告__1216621650",
#     "600415__小商品城__2022年年度报告__1216380651",
#     "600790__轻纺城__轻纺城2022年年度报告__1216613001",
#     "601601__中国太保__中国太保2022年年度报告__1216226291",
#     "601628__中国人寿__中国人寿2022年年度报告__1216260028",
#     "834021__流金科技__2022年年度报告__1216582112"
# ]

# file_title_list = [
# "605056__咸亨国际__咸亨国际：2022年年度报告__1216478420",
# # "600123__兰花科创__兰花科创2022年度报告全文__1216554351", # 应收款项不在合并资产负债表中，而在“1. 资产及负债状况 ”子表中
# # "600392__盛和资源__盛和资源2022年年度报告__1216689975", # 合并资产负债表边框有开口导致无法解析出总资产
# # "836433__大唐药业__2022年年度报告__1216619159", # 边框颜色导致无法识别合并资产负债表、合并利润表
# # "688778__厦钨新能__厦门厦钨新能源材料股份有限公司2022年年度报告__1216520043", # 134-1，135-1，136-1未识别为同一张表
# # "688121__卓然股份__2022年年度报告（修订版）__1217053279" # 129-1,130-1,131-1未识别为同一张表
# ]

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
# file_title_list = [
#     "601288__农业银行__农业银行2022年度报告__1216275777",
#     "430047__诺思兰德__2022年年度报告__1216626077",
#     "600120__浙江东方__浙江东方金融控股集团股份有限公司2022年年度报告__1216356349",
#     "603616__韩建河山__韩建河山2022年年度报告__1216646009",
#     "600901__江苏金租__江苏金租：2022年年度报告__1216523728", # 无法解析出textline
#     "600927__永安期货__永安期货股份有限公司2022年年度报告__1216626323",
#     "000415__渤海租赁__2022年年度报告__1216658168",
#     ]

# test_error_result = [
#     {
#         "file_title": "601211__国泰君安__国泰君安证券股份有限公司2022年年度报告__1216263383",
#         "reason": "合并资产负债表生成失败",# pdf格式问题
#     },
#     {"file_title": "000783__长江证券__2022年年度报告__1216691305", "reason": "合并资产负债表生成失败"},# 无合并资产负债表
#     {'file_title': '601860__紫金银行__江苏紫金农村商业银行股份有限公司2022年年度报告__1216707448', 'reason': '合并资产负债表生成失败'}, # 无合并资产负债表
#     {'文件名': '002807__江阴银行__2022年年度报告__1216234641', '生成结果': '合并利润表表失败', '原因': 'index为None'}, # 2022 年度合并利润表（除特别注明外，金额单位均为人民币千元）
#     {'文件名': '600926__杭州银行__杭州银行2022年年度报告__1216586655', '生成结果': '合并利润表表失败', '原因': 'text: 2022年度合并及银行利润表, key: 利润表 , Find_ANNOUNCE_MSG.text_line不符合数字序号或者不为空\n'}, # 合并及银行利润表
#     {'文件名': '000778__新兴铸管__2022年年度报告__1216401563', '生成结果': '合并利润表表失败', '原因': 'index为None'}, #未找到合并利润表
#     {'文件名': '601228__广州港__广州港股份有限公司2022年年度报告__1216356398', '生成结果': '合并利润表表失败', '原因': 'index为None'}, # 标题是年报，实际是年报摘要，无合并利润表
#     {'文件名': '002024__ST易购__2022年年度报告__1216700286', '生成结果': '合并利润表表失败', '原因': 'text: 2022年度合并及公司利润表, key: 合并及公司利润表 , Find_ANNOUNCE_MSG.text_line不符合数字序号或者不为空\n'},
#     {'文件名': '601186__中国铁建__中国铁建2022年年度报告__1216270071', '生成结果': '现金和现金等价物的构成表失败', '原因': 'index为None'},# 现金及现金等价物构成表名，end标志差异“年末现金及现金等价物余额”
#     {'文件名': '000423__东阿阿胶__2022年年度报告__1216219037', '生成结果': '现金和现金等价物的构成表失败', '原因': 'index为None'},# 现金及现金等价物构成表名，end标志差异“年末现金及现金等价物余额”

# ]

# parse_pdf_to_content_json()
# generate_announcement(
#     announcement_type=Financial_Statement.合并资产负债表,
#     file_title_list=file_title_list,
#     gen_table=gen_hbzcfzb,
#     use_cache=True,
#     consider_table=False,
# )
# generate_announcement(
#     announcement_type=Financial_Statement.合并利润表,
#     file_title_list=file_title_list,
#     gen_table=gen_hblrb,
#     use_cache=True,
#     consider_table=False,
# )

# filter_by_proportion_of_accounts_receivable(file_title_list)

# filter_by_increase_in_accounts_receivable(file_title_list)
# filter_by_receivable_balance(file_title_list)
# file_title = "600790__轻纺城__轻纺城2022年年度报告__1216613001"
# get_accounts_receivable(file_title)
# c = supplementing_rows_by_max_length(hbzcfzb_json)
# receivable_balance_propotion_of_monthly_average_operating_income(file_title)
# filter_by_cash_to_debt_ratio(file_title_list)
# filter_by_monetary_funds(file_title_list)
# find_standard_unqualified_opinions("688003__天准科技__2022年年度报告__1216684125")

# file_title_list_1 = filter_by_standard_unqualified_opinions(file_title_list)


# result = []
# def adapter(*kwargs):
#     seg_result = filter_by_standard_unqualified_opinions(*kwargs)
#     global result
#     result += seg_result

# concurrency2(
#     run=adapter,
#     arr=file_title_list,
#     count = 6
# )
# print('filter_by_standard_unqualified_opinions_result: ', result)
# json('static/parse-announcements/2021/filter_by_standard_unqualified_opinions.json',result)

# result1 = []
# result = json("static/parse-announcements/2021/filter_by_standard_unqualified_opinions.json")

# def adapter(*kwargs):
#     seg_result = filter_by_interest_bearing_liabilities(*kwargs)
#     global result1
#     result1 += seg_result

# concurrency2(
#     run=adapter,
#     arr=result,
#     count = 6
# )
# print("filter_by_interest_bearing_liabilities_result:", result1)
# json("static/parse-announcements/2021/filter_by_interest_bearing_liabilities.json",result1)

# result2 = []
# result1 = json("static/parse-announcements/2021/filter_by_interest_bearing_liabilities.json")
# def adapter(*kwargs):
#     seg_result = filter_by_proportion_of_accounts_receivable(*kwargs)
#     global result2
#     result2 += seg_result

# concurrency2(
#     run=adapter,
#     arr=result1,
#     count = 6
# )
# print("filter_by_proportion_of_accounts_receivable_result:", result2)
# json("static/parse-announcements/2021/filter_by_proportion_of_accounts_receivable.json",result2)

# result3 = []
# result2 = json("static/parse-announcements/2021/filter_by_proportion_of_accounts_receivable.json")
# def adapter(*kwargs):
#     seg_result = filter_by_increase_in_accounts_receivable(*kwargs)
#     global result3
#     result3 += seg_result

# concurrency2(
#     run=adapter,
#     arr=result2,
#     count = 6
# )
# print("filter_by_increase_in_accounts_receivable_result:", result3)
# json("static/parse-announcements/2021/filter_by_increase_in_accounts_receivable.json",result3)

# result4 = []
# result3 = json("static/parse-announcements/2021/filter_by_increase_in_accounts_receivable.json")
# def adapter(*kwargs):
#     seg_result = filter_by_monetary_funds(*kwargs)
#     global result4
#     result4 += seg_result

# concurrency2(
#     run=adapter,
#     arr=result3,
#     count = 6
# )
# print("filter_by_monetary_funds_result:", result4)
# json("static/parse-announcements/2021/filter_by_monetary_funds.json",result4)

# result5 = []
# result4 = json("static/parse-announcements/2021/filter_by_monetary_funds.json")
# def adapter(*kwargs):
#     seg_result = filter_by_cash_to_debt_ratio(*kwargs)
#     global result5
#     result5 += seg_result

# concurrency2(
#     run=adapter,
#     arr=result4,
#     count = 6
# )
# print("filter_by_cash_to_debt_ratio_result:", result5)
# json("static/parse-announcements/2021/filter_by_cash_to_debt_ratio.json",result5)

result6 = []
result5 = json("static/parse-announcements/2022/filter_by_cash_to_debt_ratio.json")
def adapter(*kwargs):
    seg_result = filter_by_receivable_balance(*kwargs)
    global result6
    result6 += seg_result

concurrency2(
    run=adapter,
    arr=result5,
    count = 6
)
print("filter_by_receivable_balance_result:", result6)
json("static/parse-announcements/2022/filter_by_receivable_balance.json",result6)

#601512__中新集团__中新集团2022年年度报告__1216494929

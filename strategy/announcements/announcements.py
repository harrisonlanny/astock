from service.config import (
    STATIC_ANNOUNCEMENTS_HBLRB_DIR,
    STATIC_ANNOUNCEMENTS_HBZCFZB_DIR,
    STATIC_ANNOUNCEMENTS_PARSE_DIR,
    STATIC_ANNOUNCEMENTS_XJJXJDJW_DIR,
    Financial_Statement,
)
from service.report import (
    caculate_interest_bearing_liabilities_rate,
    calculate_interest_bearing_liabilities,
    gen_cash_equivalents,
    gen_hblrb,
    gen_hbzcfzb,
    get_accounts_receivable,
    get_announcement_url,
    get_cash_and_cash_equivalents,
    get_companies_in_the_same_industry,
    get_industry,
    get_monetary_fund,
    get_operating_revenue,
    get_total_assets,
    parse_pdf,
    parse_pdf_to_content_json,
    pdf_json_url,
    propotion_of_accounts_receivable,
    receivable_balance_propotion_of_monthly_average_operating_income,
)
from utils.index import _map, get_median, get_path, is_exist, json

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
#     filter_str="where title not like '%英文%' and title not like '%取消%' and title not like '%摘要%' and title like '%2022%' ORDER BY RAND() LIMIT 100",
# )
# file_title_list = _map(r, lambda item: item["file_title"])


# 其他包含“合并资产负债表”的textline有干扰
# file_title_list = [
#     "300503__昊志机电__2022年年度报告__1216659965"
# ]

# maybe the same table 识别有误
# file_title_list = [
#     "688609__九联科技__广东九联科技股份有限公司2022年年度报告__1216647734",
#     "688553__汇宇制药__四川汇宇制药股份有限公司2022年年度报告__1216336296",
#     "601966__玲珑轮胎__山东玲珑轮胎股份有限公司2022年年度报告__1216692817",
#     "603070__万控智造__万控智造：2022年年度报告__1216454871",
#     "603256__宏和科技__宏和科技2022年年度报告__1216645622",
#     "600683__京投发展__京投发展股份有限公司2022年年度报告__1216235519",
#     "688053__思科瑞__成都思科瑞微电子股份有限公司2022年年度报告__1216362695",
#     "605162__新中港__浙江新中港热电股份有限公司2022年年度报告__1216616607",
#     "688029__南微医学__南微医学科技股份有限公司2022年年度报告__1216596450",
#     "600894__广日股份__广州广日股份有限公司2022年年度报告__1216354281",
#     "688009__中国通号__2022年年度报告__1216203728",
#     "603017__中衡设计__2022年年度报告__1216556176",
#     "601100__恒立液压__江苏恒立液压股份有限公司2022年年度报告__1216561779",
#     "601956__东贝集团__湖北东贝机电集团股份有限公司2022年年度报告__1216221059",
#     "688351__微电生理__2022年年度报告__1216245176",
#     "000816__智慧农业__2022年年度报告__1216692013", # 续表另有标题
#     "601117__中国化学__中国化学2022年年度报告__1216234431" # 续表另有标题
# ]

# 表格格式导致合并资产负债表识别不正确
# file_title_list = [
#     "688538__和辉光电__上海和辉光电股份有限公司2022年年度报告__1216623370",
#     "002500__山西证券__2022年年度报告__1216656433",
#     "000811__冰轮环境__2022年年度报告__1216390232",
#     "600928__西安银行__西安银行股份有限公司2022年年度报告__1216664133",
#     "002493__荣盛石化__2022年年度报告__1216478209",
#     "600558__大西洋__大西洋2022年年度报告__1216343924",
#     "688137__近岸蛋白__688137_2022年_年度报告__1216630973",
#     "002342__巨力索具__2022年年度报告__1216615314",
#     "838171__邦德股份__2022年年度报告__1216371491" # 表格边框颜色不一致，有的蓝色，有的黑色，取的黑色为主色，取不到蓝色的合并资产负债表
# ]


file_title_list = [
    "600481__双良节能__双良节能系统股份有限公司2022年年度报告__1216560014",
    "003005__竞业达__2022年年度报告__1216617405",
    "301098__金埔园林__2022年年度报告__1216558940",
    "002500__山西证券__2022年年度报告__1216656433",
    "002282__博深股份__2022年年度报告__1216648090",
    "603985__恒润股份__江阴市恒润重工股份有限公司2022年年度报告__1216419056",
    "002581__未名医药__2022年度报告（更正后）__1217066287",
    "688227__品高股份__2022年年度报告__1216711726",
    "601788__光大证券__光大证券股份有限公司2022年年度报告__1216279948",
    "003040__楚天龙__2022年年度报告__1216275830",
    "300668__杰恩设计__2022年年度报告__1216517939",
    "600278__东方创业__东方创业2022年度报告__1216701140",
    "831305__海希通讯__2022年年度报告（更正后）__1216861207",
    "300097__智云股份__2022年年度报告（更新后）__1216921699",
    "603167__渤海轮渡__渤海轮渡集团股份有限公司2022年年度报告__1216441667",
    "688510__航亚科技__无锡航亚科技股份有限公司2022年年度报告__1216582882",
    "688288__鸿泉物联__鸿泉物联：2022年年度报告__1216687349",
    "600500__中化国际__中化国际2022年年度报告__1216663256",
    "603003__龙宇股份__龙宇股份2022年年度报告__1216645157",
    "688538__和辉光电__上海和辉光电股份有限公司2022年年度报告__1216623370",
]



def generate_hblrb(file_title_list, use_cache: bool = True):
    # 1. 遍历所有的file_title_list,并parse_pdf (已经parse过就不会再parse!!)
    for file_title in file_title_list:
        file_url = get_announcement_url(file_title)
        parse_pdf(file_url, file_title, use_cache)

    # 2. 遍历所有的file_title_list，根据table.json和content.json来生成合并利润表
    error_file_title_list = []
    for file_title in file_title_list:
        # 缺乏必要的json，而无法合成利润表等明细表的file_title_list
        hblrb_json_url = f"{STATIC_ANNOUNCEMENTS_HBLRB_DIR}/{file_title}__{Financial_Statement.合并利润表.value}.json"
        table_json_url = f"{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{file_title}__table.json"
        content_json_url = (
            f"{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{file_title}__content.json"
        )

        # 1. 检查是否存在合并利润表，如果不存在，才进行合成
        if use_cache and is_exist(get_path(hblrb_json_url)):
            continue
        # 2. 如果需要合成，检查必要的合成元素 table.json和content.json是否存在，如果存在 才进行合成，如果不存在
        # 可以收集异常的数据，并返回
        all_exists = is_exist(get_path(table_json_url)) and is_exist(
            get_path(content_json_url)
        )
        if use_cache and not all_exists:
            error_file_title_list.append(
                {"file_title": file_title, "reason": "缺少table.json或content.json"}
            )
            continue
        # 3. 进行合成
        gen_success = gen_hblrb(file_title, hblrb_json_url)
        if not gen_success:
            error_file_title_list.append(
                {"file_title": file_title, "reason": "table.json中没找到合并利润表"}
            )
    print("合并利润表有问题的file_title_list: ", error_file_title_list)
    return error_file_title_list


def generate_xjjxjdjw(file_title_list, use_cache: bool = True):
    # 1. 遍历所有的file_title_list,并parse_pdf (已经parse过就不会再parse!!)
    for file_title in file_title_list:
        file_url = get_announcement_url(file_title)
        parse_pdf(file_url, file_title, use_cache)

    # 2. 遍历所有的file_title_list，根据table.json和content.json来生成现金及现金等价物表
    error_file_title_list = []
    for file_title in file_title_list:
        # 缺乏必要的json，而无法合成利润表等明细表的file_title_list
        xjjxjdjw_json_url = f"{STATIC_ANNOUNCEMENTS_XJJXJDJW_DIR}/{file_title}__{Financial_Statement.现金和现金等价物的构成.value}.json"
        table_json_url = f"{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{file_title}__table.json"
        content_json_url = (
            f"{STATIC_ANNOUNCEMENTS_PARSE_DIR}/{file_title}__content.json"
        )

        # 1. 检查是否存在现金及现金等价物表，如果不存在，才进行合成
        if use_cache and is_exist(get_path(xjjxjdjw_json_url)):
            continue
        # 2. 如果需要合成，检查必要的合成元素 table.json和content.json是否存在，如果存在 才进行合成，如果不存在
        # 可以收集异常的数据，并返回
        all_exists = is_exist(get_path(table_json_url)) and is_exist(
            get_path(content_json_url)
        )
        if use_cache and not all_exists:
            error_file_title_list.append(
                {"file_title": file_title, "reason": "缺少table.json或content.json"}
            )
            continue
        # 3. 进行合成
        gen_success = gen_cash_equivalents(file_title, xjjxjdjw_json_url)
        if not gen_success:
            error_file_title_list.append(
                {"file_title": file_title, "reason": "table.json中没找到现金及现金等价物表"}
            )
    print("现金及现金等价物表有问题的file_title_list: ", error_file_title_list)
    return error_file_title_list


def filter_by_interest_bearing_liabilities(file_title_list):
    # 筛选有息负债符合条件的公司
    error_file_title_list = generate_hbzcfzb(file_title_list)
    error_list = _map(error_file_title_list, lambda item: item["file_title"])
    file_title_list = list(set(file_title_list) - set(error_list))
    companies = []
    for file_title in file_title_list:
        hbzcfzb_json_url = f"{STATIC_ANNOUNCEMENTS_HBZCFZB_DIR}/{file_title}__{Financial_Statement.合并资产负债表.value}.json"
        try:
            file_content = json(hbzcfzb_json_url)
            try:
                result = caculate_interest_bearing_liabilities_rate(
                    calculate_interest_bearing_liabilities(file_title),
                    get_total_assets(file_title),
                )
                # print(f"{file_title}的有息负债占比为{result}%")
                if result < 60:
                    companies.append(file_title)
            except:
                print(f"{file_title}无法计算有息负债占比")
        except:
            print(f"{file_title}未找到合并资产负债表")
    print("符合有息负债占比条件的公司有：", companies)
    # r = len(companies)/len(file_title_list)*100
    # print(f"符合有息负债占比筛选条件的公司比例为：{r}%")
    print(f"符合有息负债占比筛选条件的公司比例为：{len(companies)/len(file_title_list)*100}%")


def filter_by_proportion_of_accounts_receivable(file_title_list):
    """
    筛选应收占比符合条件的公司
    """
    error_file_title_list = generate_hbzcfzb(file_title_list)
    error_list = _map(error_file_title_list, lambda item: item["file_title"])
    file_title_list = list(set(file_title_list) - set(error_list))
    target = []
    for file_title in file_title_list:
        try:
            propotion = propotion_of_accounts_receivable(file_title)
            print(f"{file_title}应收占比为{propotion}%")
            if propotion < 30:
                target.append(file_title)
            else:
                print(f"{file_title}应收占比大于等于30%，不符合条件")
        except:
            print(f"总资产项目不存在！{file_title}无法计算应收占比")

    rate = len(target) / len(file_title_list) * 100
    print(f"符合应收占比条件的公司有：{target}，比例为{rate}%")


def filter_by_increase_in_accounts_receivable(file_title_list):
    target = []
    abnormal_count = 0
    for file_title in file_title_list:
        try:
            # 1.从合并资产负债表中获取应收款增长率
            accounts_receivable_success = get_accounts_receivable(file_title)
            if accounts_receivable_success:
                growth_rate_of_account_receivable = accounts_receivable_success[2]
            # 2.从合并利润表中获取营业收入增长率
            operating_revenue_success = get_operating_revenue(file_title)
            if operating_revenue_success:
                growth_rate_of_operating_revenue = operating_revenue_success[2]
            # 3.比较应收款增长率和营业收入增长率，若应收款增幅<营业收入增幅，则符合条件
            if accounts_receivable_success and operating_revenue_success:
                if growth_rate_of_account_receivable < growth_rate_of_operating_revenue:
                    target.append(file_title)
        except:
            print(f"{file_title}缺少必要的合并资产负债表或合并利润表")
            abnormal_count += 1
    print(f"符合应收款增幅<营业收入增幅的比例是{len(target)/(len(file_title_list)-abnormal_count)*100}%")
    return target


def filter_by_receivable_balance(file_title_list):
    target = []
    for file_title in file_title_list:
        companies_info = {}
        # 应收账款余额/月均营业收入，越小越好（与同行业公司比较，处于中位数以下）
        same_industry_propotion = []
        # 1.遍历，计算传入的公司的应收账款/月均营业收入，获取每个公司所在的行业
        industry = list(get_industry([file_title]).values())
        same_industry_companies = get_companies_in_the_same_industry(
            file_title, industry
        )
        if same_industry_companies[0]:
            generate_hblrb(same_industry_companies[0])
            generate_hbzcfzb(same_industry_companies[0])
            for company in same_industry_companies[0]:
                propotion = (
                    receivable_balance_propotion_of_monthly_average_operating_income(
                        company
                    )
                )
                company_info = {company: propotion}
                companies_info.update(company_info)
            for key in companies_info:
                if companies_info[key] == None:
                    companies_info[key] = 0
            median = get_median(companies_info.values())
            if companies_info[file_title] < median:
                target.append(file_title)
            else:
                print(f"{file_title}不符合应收账款/月均营业收入<中位数条件！")
            print(f"符合应收账款/月均营业收入<中位数条件的公司有：{target}")
            print(f"符合应收账款/月均营业收入<中位数条件的公司比例为{len(target)/len(file_title_list)*100}%")


def filter_by_monetary_funds(file_title_list):
    """
    货币资金大于等于有息负债
    """
    target = []
    for file_title in file_title_list:
        hbzcfzb_json_url = f"{STATIC_ANNOUNCEMENTS_HBZCFZB_DIR}/{file_title}__{Financial_Statement.合并资产负债表.value}.json"
        try:
            file_content = json(hbzcfzb_json_url)
            # 1.获取合并资产负债表中的货币资金
            current_monetary_funds = get_monetary_fund(file_title)
            # 2.获取合并资产负债表中的有息负债
            current_interest_bearing_liabilities = (
                calculate_interest_bearing_liabilities(file_content)
            )
            # 3.将符合条件的公司加入target
            if current_monetary_funds >= current_interest_bearing_liabilities:
                target.append(file_title)
        except:
            print(f"{file_title}未生成合并资产负债表")
    print(f"{target}符合货币资金大于等于有息负债的条件")
    print(f"符合货币资金大于等于有息负债条件的公司比例为{len(target)/len(file_title_list)*100}%")


def filter_by_cash_to_debt_ratio(file_title_list):
    """
    从安全性角度出发，筛选 现金债务比=现金及现金等价物/有息负债>1的公司
    """
    target = []
    for file_title in file_title_list:
        hbzcfzb_json_url = f"{STATIC_ANNOUNCEMENTS_HBZCFZB_DIR}/{file_title}__{Financial_Statement.合并资产负债表.value}.json"
        try:
            file_content = json(hbzcfzb_json_url)
            # 1.获取现金和现金等价物的构成表中的期末现金及现金等价物余额
            current_cash_equivalents = get_cash_and_cash_equivalents(file_title)
            # 2.获取合并资产负债表中的有息负债
            current_interest_bearing_liabilities = (
                calculate_interest_bearing_liabilities(file_title)
            )
            # 3.将符合条件的公司加入target
            if current_cash_equivalents >= current_interest_bearing_liabilities:
                target.append(file_title)
        except:
            print(f"{file_title}未找到合并资产负债表")
    print(f"{target}符合现金债务比>1的条件")
    print(f"符合现金债务比>1的公司比例为{len(target)/len(file_title_list)*100}%")

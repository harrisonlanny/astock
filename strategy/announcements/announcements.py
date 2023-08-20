import threading
from db.index import read_table
from service.config import (
    STATIC_ANNOUNCEMENTS_HBLRB_DIR,
    STATIC_ANNOUNCEMENTS_HBZCFZB_DIR,
    STATIC_ANNOUNCEMENTS_PARSE_DIR,
    STATIC_ANNOUNCEMENTS_XJJXJDJW_DIR,
    Financial_Statement,
)
from service.report import (
    caculate_gross_margin,
    caculate_interest_bearing_liabilities_rate,
    caculate_ratio_of_expense_and_gross,
    calculate_interest_bearing_liabilities,
    find_standard_unqualified_opinions,
    gen_cash_equivalents,
    gen_hblrb,
    gen_hbzcfzb,
    gen_zyyw,
    generate_announcement,
    get_accounts_receivable,
    get_announcement_url,
    get_cash_and_cash_equivalents,
    get_companies_in_the_same_industry,
    get_industry,
    get_management_expense,
    get_monetary_fund,
    get_operating_revenue,
    get_total_assets,
    parse_pdf,
    parse_pdf_to_content_json,
    pdf_json_url,
    propotion_of_accounts_receivable,
    receivable_balance_propotion_of_monthly_average_operating_income,
)
from utils.index import _filter, _map, get_median, get_path, is_exist, json, json2


def filter_by_standard_unqualified_opinions(file_title_list):
    '''
    筛选符合“标准无保留意见”条件的公司
    '''
    target = []
    for index, file_title in enumerate(file_title_list):
        print(f"{file_title} 【{threading.current_thread().name}:{index + 1}/{len(file_title_list)}】")
        file_url = get_announcement_url(file_title)
        parse_pdf_to_content_json(file_url, file_title)
        if find_standard_unqualified_opinions(file_title):
            target.append(file_title)
    # print(f"不符合“标准无保留意见”条件的公司有：{list(set(file_title_list)-set(target))}")
    return target
    # print(f"符合“标准无保留意见”条件的公司有：{target}，所占比例为{len(target)/len(file_title_list)*100}%")


def filter_by_interest_bearing_liabilities(file_title_list):
    # 筛选有息负债符合条件的公司
    error_file_title_list = generate_announcement(
        announcement_type=Financial_Statement.合并资产负债表,
        file_title_list=file_title_list,
        gen_table=gen_hbzcfzb,
        use_cache=True,
        consider_table=False,
    )
    error_list = _map(error_file_title_list, lambda item: item["文件名"])
    json('static/parse-announcements/2022/error/interest_bearing_liabilities.json', error_file_title_list)
    file_title_list = list(set(file_title_list) - set(error_list))
    companies = []
    for file_title in file_title_list:
        print(f"{file_title}")
        hbzcfzb_json_url = f"{STATIC_ANNOUNCEMENTS_HBZCFZB_DIR}/{file_title}__{Financial_Statement.合并资产负债表.value}.json"
        try:
            json(hbzcfzb_json_url)
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
    print(f"符合有息负债占比筛选条件的公司比例为：{len(companies)/len(file_title_list)*100}%")
    return companies

def filter_by_proportion_of_accounts_receivable(file_title_list):
    """
    筛选应收占比符合条件的公司
    """
    error_file_title_list = generate_announcement(
        announcement_type=Financial_Statement.合并资产负债表,
        file_title_list=file_title_list,
        gen_table=gen_hbzcfzb,
        use_cache=True,
        consider_table=False,
    )
    error_list = _map(error_file_title_list, lambda item: item["文件名"])
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
    return target


def filter_by_increase_in_accounts_receivable(file_title_list):
    target = []
    abnormal_count = 0
    error_file_title_list = \
    generate_announcement(
        announcement_type=Financial_Statement.合并利润表,
        file_title_list=file_title_list,
        gen_table=gen_hblrb,
        use_cache=True,
        consider_table=False,).extend(
        generate_announcement(
        announcement_type=Financial_Statement.合并资产负债表,
        file_title_list=file_title_list,
        gen_table=gen_hbzcfzb,
        use_cache=True,
        consider_table=False,))
    error_list = _map(error_file_title_list, lambda item: item["文件名"])
    # json('static/parse-announcements/2022/error/increase_in_accounts_receivable.json', error_file_title_list)
    file_title_list = list(set(file_title_list) - set(error_list))
    for file_title in file_title_list:
        try:
            # 1.从合并资产负债表中获取应收款增长率
            accounts_receivable_success = get_accounts_receivable(file_title)
            if accounts_receivable_success:
                growth_rate_of_account_receivable = accounts_receivable_success[1]
            # 2.从合并利润表中获取营业收入增长率
            operating_revenue_success = get_operating_revenue(file_title)
            if operating_revenue_success:
                growth_rate_of_operating_revenue = operating_revenue_success[1]
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
    # 1.按行业计算应收账款/月均营业收入中位数
    all_industries = _map(read_table(
            table_name="stock_basic",
            fields=["distinct industry"],
            result_type="dict"
        ), lambda item: item["distinct industry"])
        # , _filter(lambda item: item is not None)
    
    industry_propotion_info_dict = {}
    for index, industry in enumerate(all_industries):
        print(f"行业进度:{index+1}/{len(all_industries)}")
        companies = _map(read_table(
            table_name="stock_basic",
            fields=["name"],
            result_type="dict",
            filter_str=f"where industry = '{industry}'"
        ), lambda item: item["name"])

        industry_file_title_list = []
        industry_receivable_balance_propotion = []
        
        for index,company in enumerate(companies):
            print(f"公司进度：{index+1}/{len(companies)}")
            file_title = _map(read_table(
                table_name="announcements",
                fields=["file_title"],
                result_type="dict",
                filter_str=f"where name = '{company}' and title not like '%英文%' and title not like '%取消%' and title not like '%摘要%' and title not like '%公告%' and title not like '%修订前%' and title like '%2022%'"
                ), lambda item: item["file_title"])
            if file_title:
                industry_file_title_list.append(file_title[0])
        
            generate_announcement(
            announcement_type=Financial_Statement.合并资产负债表,
            file_title_list=industry_file_title_list,
            gen_table=gen_hbzcfzb,
            use_cache=True,
            consider_table=False,
    )
            generate_announcement(
                announcement_type=Financial_Statement.合并利润表,
                file_title_list=industry_file_title_list,
                gen_table=gen_hblrb,
                use_cache=True,
                consider_table=False,
            )
            if file_title:
                propotion = receivable_balance_propotion_of_monthly_average_operating_income(file_title[0])
            industry_receivable_balance_propotion.append(propotion)
            industry_propotion_info = {
                industry: industry_receivable_balance_propotion
            }
            industry_propotion_info_dict.update(industry_propotion_info)
    # 2.比较当前file与行业中位数关系，符合条件的加入target
    for file_title in file_title_list:
        industry = _map(read_table(
            table_name="stock_basic",
            fields=["industry"],
            result_type="dict",
            filter_str=f"where symbol = '{file_title[:6]}'"
        ), lambda item: item["industry"])[0]
        if industry is not None:
            industry_median = get_median(industry_propotion_info_dict[f"{industry}"])
        else:
            industry_median = 0
        current_rate = receivable_balance_propotion_of_monthly_average_operating_income(file_title)
        if current_rate < industry_median:
            target.append(file_title)
    return target


def filter_by_monetary_funds(file_title_list):
    """
    货币资金大于等于有息负债
    """
    target = []
    generate_announcement(
    announcement_type=Financial_Statement.合并资产负债表,
    file_title_list=file_title_list,
    gen_table=gen_hbzcfzb,
    use_cache=True,
    consider_table=False,
)
    for file_title in file_title_list:
        try:
            # 1.获取合并资产负债表中的货币资金
            current_monetary_funds = get_monetary_fund(file_title)
            # 2.获取合并资产负债表中的有息负债
            current_interest_bearing_liabilities = (
                calculate_interest_bearing_liabilities(file_title)
            )
            # 3.将符合条件的公司加入target
            if current_monetary_funds >= current_interest_bearing_liabilities:
                target.append(file_title)
        except:
            print(f"{file_title}未生成合并资产负债表")
    print(f"{target}符合货币资金大于等于有息负债的条件")
    print(f"符合货币资金大于等于有息负债条件的公司比例为{len(target)/len(file_title_list)*100}%")
    return target 

def filter_by_cash_to_debt_ratio(file_title_list):
    """
    从安全性角度出发，筛选 现金债务比=现金及现金等价物/有息负债>1的公司
    """
    target = []
    generate_announcement(
    announcement_type=Financial_Statement.现金和现金等价物的构成,
    file_title_list=file_title_list,
    gen_table=gen_cash_equivalents,
    use_cache=True,
    consider_table=False,
)
    for file_title in file_title_list:
        try:
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
            print(f"{file_title}未找到现金及现金等价物表")
    print(f"{target}符合现金债务比>1的条件")
    print(f"符合现金债务比>1的公司比例为{len(target)/len(file_title_list)*100}%")
    return target

def filter_by_gross_margin(file_title_list):
    """
    筛选毛利率大于40%的公司
    """
    target = []
    generate_announcement(
    announcement_type=Financial_Statement.营业收入和营业成本,
    file_title_list=file_title_list,
    gen_table=gen_zyyw,
    use_cache=True,
    consider_table=False,
)
    for file_title in file_title_list:
        try:
            gross_margin = caculate_gross_margin(file_title)
            if gross_margin >= 0.4:
                target.append(file_title)
        except:
            print(f"{file_title}未找到主营业务表")
    print(f"{target}符合毛利率>40%的条件")
    print(f"符合毛利率>40%的公司比例为{len(target)/len(file_title_list)*100}%")
    return target

def filter_by_growth_rate_of_management_expense(file_title_list):
    """
    筛选管理费用增幅<=营业收入增幅的公司
    """
    target = []
    generate_announcement(
    announcement_type=Financial_Statement.合并利润表,
    file_title_list=file_title_list,
    gen_table=gen_hblrb,
    use_cache=True,
    consider_table=False,
)
    for file_title in file_title_list:
        try:
            management_expense_growth = get_management_expense(file_title)[1]
            operating_revenue_growth = get_operating_revenue(file_title)[1]
            if management_expense_growth <= operating_revenue_growth:            
                target.append(file_title)
        except:
            print(f"{file_title}未找到合并利润表")
    print(f"{target}符合管理费用增幅<=营业收入增幅的条件")
    print(f"符合管理费用增幅<=营业收入增幅的公司比例为{len(target)/len(file_title_list)*100}%")
    return target

def filter_by_ratio_of_expense_and_gross(file_title_list):
    target = []
    generate_announcement(
    announcement_type=Financial_Statement.营业收入和营业成本,
    file_title_list=file_title_list,
    gen_table=gen_zyyw,
    use_cache=True,
    consider_table=False,
)
    generate_announcement(
    announcement_type=Financial_Statement.合并利润表,
    file_title_list=file_title_list,
    gen_table=gen_hblrb,
    use_cache=True,
    consider_table=False,
)
    for file_title in file_title_list:
        try:
            ratio = caculate_ratio_of_expense_and_gross(file_title)
            if ratio <=0.7:
                target.append(file_title)
                print(f"{file_title}费用/毛利润有竞争力！")
        except:
            print(f"{file_title}无法计算费用/毛利润比值！")
    print(f"{target}符合费用/毛利润<70%的条件")
    print(f"符合费用/毛利润<70%的公司比例为{len(target)/len(file_title_list)*100}%")
    return target
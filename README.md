复权：前降后升

TODOLIST:
- 20230519
  - 写db工具函数 update_table_fields: 修改表结构
  - 写service函数 ：update_d_tables: 遍历数据库d表，检查表如果空，则请求api接口填充该表；如果非空，拿最新的数据日期,最后对齐请求结束
  
d_000536表为空，调用fetch_daily(000536.SZ) 143/5383
query daily from  to 
6000
query daily from 19940810 to 19940810
1
end_date 19940809
query daily from  to 19940809
169
api返回字段排序后： ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount', 'adj_factor']
DELETE FROM d_000536


d_001324表为空，调用fetch_daily(001324.SZ) 566/5383
query daily from  to 
0
fields: ['ts_code' 'trade_date' 'open' 'high' 'low' 'close' 'pre_close' 'change'
 'pct_chg' 'vol' 'amount' 'adj_factor']
DELETE FROM d_001324
pymysql.err.ProgrammingError: (1064, "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '' at line 1")


天齐锂业 预计涨到96，第一波涨到88.7

巨潮资讯网(看财报)
http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&lastPage=index

## 快速阅读资产负债表
### 钱的来源
1.财报最后一行“负债和所有者权益合计”-有多少家当
2.财报倒数第二行“所有者权益合计”-有多少是自己的
3.1、2相减-负债
4.看负债项目，找出有疑虑的项目。例如：为何有钱却欠着税款不交？-搜索“应交税费”
5.找出应交税费明细
6.计算企业负债率：有息负债/总资产，与同时期、同行业的公司比较
7.有息负债/总资产>=60%，应小心
### 钱的去处
1.资产总计，对与上一期财报的增长有个大概印象
2.四个要点：生产资产/总资产（评价公司类型：重公司/轻公司）、应收/总资产、货币资金/有息负债、非主业资产/总资产；三个角度：结构、历史、同行
3.生产资产/总资产
结构：
  当年税前利润总额/生产资产（生产资产：固定资产+在建工程+工程物资+无形资产里的土地），得出的比值显著高于社会平均资本回报率【银行贷款标准利率的两倍】，轻资产>重资产；
历史、同行：
  看生产资产/总资产如何变化，竞争对手如何变化，若一致则未发生转折，否则蕴藏机会或陷阱。思考变化的原因；
4.应收/总资产
应收 = 资产负债表所有带“应收”两个字的科目数字总和-银行承兑汇票金额
1）应收/总资产，不应大于30%；
2）应收账款余额/月均营业收入，越小越好（与同行业公司比较，处于中位数以下）；
3）除基数较小的情况，不应发生超过营业收入增幅的应收款增幅；
4）观察异常：某些应收款单独测试减值为0，应收款集中在少数关联企业，其他应收款科目突然大幅增加
5.货币资金/有息负债（偿债能力）
有息负债：融资性负债（短期借款、交易性金融负债、长期借款、应付债券、一年内到期的非流动负债等）一般都是有息负债，而经营性负债和分配性负债则不是
货币资金>=有息负债
可放宽：货币资金+随时可变现的交易性金融资产、应收票据>=有息负债
可纵向比较：看看企业历史发生的变化
6.非主业资产/总资产
非主业资产：如制造业企业的交易性金融资产、可供出售金融资产、持有至到期投资、银行理财产品或投资性房地产（与主业经营无关的资产）
比例增加，说明公司管理层认为在自己的行业内已经很难发现有潜力的投资机会
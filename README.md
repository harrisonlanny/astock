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
import pymysql

from mysql import get_shareholder

db = pymysql.connect(host='localhost',
                     user='root',
                     password='HaiHai211!',
                     database='astock')
# 创建游标对象
cursor = db.cursor()

# 查询ts_code列表
ts_code = "SELECT ts_code FROM stock_basic"
ts_lst = []
cursor.execute(ts_code)  # 执行sql语句，也可执行数据库命令，如：show tables
result = cursor.fetchall()  # 所有结果
for ts in result:
    ts = ts[0]
    ts_lst.append(ts)
cursor.close()  # 关闭当前游标
db.close()  # 关闭数据库连接

# 查询所有股票十大股东
for ts in ts_lst:
    get_shareholder(ts)

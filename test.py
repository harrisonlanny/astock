
from service.index import create_new_d_tables,get_current_d_tables,api_query

columns, data = api_query('stock_basic', list_status='D')
print(data)



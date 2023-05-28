import math
from datetime import datetime, date
from markupsafe import escape
from flask import Flask, request
from flask_cors import CORS
# from flask.json import jsonify, dumps
from flask.json.provider import DefaultJSONProvider

from db.index import read_table, get_total
import json

from utils.stock import d_to_w


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        # print('MyEncoder: ', obj, type(obj))
        if isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return super().default(obj)

        # else:
        #     return json.JSONEncoder.default(self, obj)


print(__name__)
app = Flask(__name__)
cors = CORS(app)


# app.json = MyEncoder(app)


# app.config['JSON_AS_ASCII'] = False
# app.config['PORT'] = 6000


def _json(data):
    json_str = json.dumps(data, ensure_ascii=False, cls=MyEncoder).encode('utf8')
    return app.response_class(json_str, mimetype="application/json")


def get_page_info():
    current = int(request.args.get("current"))
    page_size = int(request.args.get("pageSize"))
    limit_sql = f"LIMIT {(current - 1) * page_size}, {page_size}"
    return {
        "current": current,
        "page_size": page_size,
        "limit_sql": limit_sql
    }


def page_response(data, total: int, current: int, page_size: int):
    page_sum = math.ceil(total / page_size)
    next = current < page_sum
    return _json({
        "data": data,
        "total": total,
        "current": current,
        "page_size": page_size,
        "page_sum": page_sum,
        "next": next
    })


def get_search_info():
    args = request.args
    sqls = []
    for arg in args:
        if arg in ['current', 'pageSize']:
            continue
        value = args.get(arg)
        item_sql = f"`{arg}` like '%{value}%'"
        sqls.append(item_sql)
    if len(sqls) == 0:
        return ""
    where_sql = "WHERE " + " AND ".join(sqls)
    return where_sql


@app.route("/")
def hello_world():
    return "<p>孙正harrison lanny peace & rich</p>"


@app.route("/stock", methods=['GET'])
def stocks():
    print('request', request.args)
    page_info = get_page_info()
    current = page_info.get("current")
    page_size = page_info.get("page_size")
    limit_sql = page_info.get('limit_sql')
    where_sql = get_search_info()

    count_str = f"{where_sql}".strip()
    filter_str = f"{where_sql} {limit_sql}".strip()
    print('filter_str', filter_str)
    total = get_total('stock_basic', filter_str=count_str)
    data = read_table('stock_basic', result_type='dict', filter_str=filter_str)
    return page_response(data, total=total, page_size=page_size, current=current)


@app.route("/stock/<symbol>")
def stock(symbol):
    # return f'获取 {symbol}对应的所有日线数据(从数据库)'
    symbol = escape(symbol)
    result = read_table("stock_basic", filter_str=f"WHERE `symbol`='{symbol}'", result_type='dict')
    return _json(result)


@app.route("/d/<symbol>")
def d(symbol):
    # return f'获取 {symbol}对应的所有日线数据(从数据库)'
    symbol = escape(symbol)
    table_name = f"d_{symbol}"
    print('table_name', table_name)
    data = read_table(table_name, result_type='dict')
    return _json(data)


@app.route("/w/<symbol>")
def w(symbol):
    # return f'获取 {symbol}对应的所有周线数据(从数据库)'
    symbol = escape(symbol)
    table_name = f"d_{symbol}"
    data = read_table(table_name, result_type='dict')
    data = d_to_w(data, date_format="%Y-%m-%d")
    return _json(data)


# @app.route('/post/<int:post_id>')
# def show_post(post_id):
#     # show the post with the given id, the id is an integer
#     return f'Post {post_id}'
#
#
# @app.route('/path/<path:subpath>')
# def show_subpath(subpath):
#     # show the subpath after /path/
#     return f'Subpath {escape(subpath)}'

if __name__ == '__main__':
    # app.config['JSON_AS_ASCII'] = False
    app.run(port=8888)

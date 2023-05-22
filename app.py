from datetime import datetime, date

from flask import Flask
# from flask.json import jsonify, dumps
from flask.json.provider import DefaultJSONProvider
from markupsafe import escape
from db.index import read_table
import json


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


# app.json = MyEncoder(app)


# app.config['JSON_AS_ASCII'] = False
# app.config['PORT'] = 6000


def _json(data):
    json_str = json.dumps(data, ensure_ascii=False, cls=MyEncoder).encode('utf8')
    return app.response_class(json_str, mimetype="application/json")


@app.route("/")
def hello_world():
    return "<p>孙正harrison lanny peace & rich</p>"


@app.route("/stock")
def stocks():
    result = read_table("stock_basic", result_type='dict')
    return _json(result)


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
    return _json(read_table(table_name, result_type='dict'))


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

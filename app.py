from flask import Flask
from markupsafe import escape
from db.index import read_table

print(__name__)
app = Flask(__name__)


@app.route("/")
def hello_world():
    return "<p>harrison lanny peace & rich</p>"


@app.route("/d/<symbol>")
def d(symbol):
    # return f'获取 {symbol}对应的所有日线数据(从数据库)'
    table_name = f"d_{symbol}"
    return read_table(table_name, result_type='dict')
# @app.route('/user/<username>')
# def show_user_profile(username):
#     # show the user profile for that user
#     return f'User {escape(username)}'


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

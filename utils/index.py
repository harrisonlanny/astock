import os
from constants import PROJECT_NAME


def get_current_path():
    return os.path.abspath(os.path.dirname(__file__))


def get_root_path():
    cur_path = get_current_path()
    index = cur_path.find(PROJECT_NAME) + len(PROJECT_NAME)
    return cur_path[:index]


def get_path(src_relative_path):
    str_path = str(src_relative_path)
    _path = str_path if str_path.startswith('/') else '/' + str_path
    return f"{get_root_path()}{_path}"



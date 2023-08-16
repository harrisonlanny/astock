from enum import Enum


class Announcement_Category(Enum):
    一季报="一季报" #当年4月1日-4月30日
    半年报="半年报" #当年7月1日-8月30日
    三季报="三季报" #当年10月1日-10月31日
    年报="年报" #次年1月1日-次年4月30日

Announcement_Category_Options = {
    Announcement_Category.一季报.value: {
        "juchao_category": "category_yjdbg_szsh"
    },
    Announcement_Category.半年报.value: {
        "juchao_category": "category_bndbg_szsh"
    },
    Announcement_Category.三季报.value: {
        "juchao_category": "category_sjdbg_szsh"
    },
    Announcement_Category.年报.value: {
        "juchao_category": "category_ndbg_szsh"
    },
}

class Financial_Statement(Enum):
    合并资产负债表 = "合并资产负债表"
    合并利润表 = "合并利润表"
    现金和现金等价物的构成 = "现金和现金等价物的构成"
    营业收入和营业成本 = "营业收入和营业成本"

Financial_Statement_DataSource = {
    Financial_Statement.合并资产负债表.value: {
        "keywords": [
            "合并资产负债表",
            "合并及公司资产负债表",
            "合并资产负债表和资产负债表",
            "资产负债表",
        ],
        "path": "/static/parse-announcements/hbzcfzb"
    },
    Financial_Statement.合并利润表.value: {
        "keywords": [
            "合并利润表",
            "合并及公司利润表",
            "利润表",
        ],
        "path": "/static/parse-announcements/hblrb"
    },
    Financial_Statement.现金和现金等价物的构成.value: {
        "keywords": [
            "现金和现金等价物的构成",
            "现金及现金等价物",
            "现金及现金等价物的构成",
            "现金和现金等价物构成情况",
            "现金和现金等价物"
        ],
        "path": "/static/parse-announcements/xjjxjdjw"
    },
    Financial_Statement.营业收入和营业成本.value: {
        "keywords": [
            "营业收入和营业成本",
            "营业收入和营业成本情况"
        ],
        "path": "/static/parse-announcements/zyyw"
    }
}


class Find_ANNOUNCE_MSG(Enum):
    index为None="index为None"
    text_line不符合数字序号或者不为空="text_line不符合数字序号或者不为空"

STATIC_ANNOUNCEMENTS_DIR = "/static/announcements"
STATIC_ANNOUNCEMENTS_PARSE_DIR = "/static/parse-announcements/base"
STATIC_ANNOUNCEMENTS_HBZCFZB_DIR = "/static/parse-announcements/hbzcfzb"
STATIC_ANNOUNCEMENTS_HBLRB_DIR = "/static/parse-announcements/hblrb"
STATIC_ANNOUNCEMENTS_XJJXJDJW_DIR="/static/parse-announcements/xjjxjdjw"
STATIC_ANNOUNCEMENTS_ZYYW_DIR = "/static/parse-announcements/zyyw"

JU_CHAO_PROTOCOL = "http://"
JU_CHAO_HOST = "www.cninfo.com.cn"
JU_CHAO_STATIC_HOST = "static.cninfo.com.cn"
JU_CHAO_BASE_URL = f"{JU_CHAO_PROTOCOL}{JU_CHAO_HOST}"
JU_CHAO_STATIC_URL = f"{JU_CHAO_PROTOCOL}{JU_CHAO_STATIC_HOST}"

JU_CHAO_COOKIE = {
    "JSESSIONID": "7BE08DF899613177F426827B251A630E",
    "insert_cookie": "45380249",
    "routeId": ".uc1",
    "_sp_ses.2141": "*",
    "_sp_id.2141": "69ba6006-a96c-4e68-8da2-1f3a56e943f2.1685969405.4.1686988217.1686383617.1042ce01-5319-4d64-a642-8b6dbd468a75",
}
JU_CHAO_HEADERS = {
    "Host": "www.cninfo.com.cn",
    "Origin": "http://www.cninfo.com.cn",
    "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&lastPage=index",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
}






# 给column中的Column_name加上反引号
def safe_column(column: str):
    arr = column.split(' ')
    arr[0] = f"`{arr[0]}`"
    result = " ".join(arr)
    return result


FIELD_TYPES = ['TINYINT', 'SMALLINT', 'MEDIUMINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL',
               'DATE', 'TIME', 'YEAR', 'DATETIME', 'TIMESTAMP',
               'CHAR', 'VARCHAR', 'TINYBLOB', 'TINYTEXT', 'BLOB', 'TEXT', 'MEDIUMBLOB', 'MEDIUMTEXT', 'LONGBLOB',
               'LONGTEXT']
FIELD_KEYWORDS = FIELD_TYPES + ['NULL', 'NOT NULL', 'PRIMARY KEY']


# field: "trade_date"
# field_desc: "DATE NOT NULL"
# field_define = field + " " + field_desc

def is_field_define(field_define: str):
    arr = field_define.split(' ')
    return len(arr) > 1


def safe_field(field: str):
    return f"`{field.replace('`', '')}`"


# field_define: jack CHAR(1) NOT NULL
def safe_field_define(field_define: str):
    if not is_field_define(field_define):
        return field_define
    field = get_field_from_define(field_define)
    field_desc = get_field_desc_from_define(field_define)

    return f"{safe_field(field)} {field_desc}"


def get_field_from_define(field_define: str):
    return field_define.split(" ")[0]


def get_field_desc_from_define(field_define: str):
    return " ".join(field_define.split(' ')[1:])

# "jack" -> field
# ""

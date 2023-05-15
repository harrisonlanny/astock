# describe -> columns
def describe_to_columns(describe: str):
    return None

# 给column中的Column_name加上反引号
def safe_column(column: str):
    arr = column.split(' ')
    arr[0] = f"`{arr[0]}`"
    result = " ".join(arr)
    return result




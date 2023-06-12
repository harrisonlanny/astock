from utils.index import json

data = {
    "name": "harrison",
    "age": 30,
    "love": ["lanny", "bozi"]
}
result = json('/test.json', data)
print(result)

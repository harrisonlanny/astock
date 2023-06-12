from utils.index import json

data = {
    "name": "harrison",
    "age": 29,
    "love": ["lanny", "bozi"]
}
json('/test.json', data)
import json
import boto3
from decimal import Decimal
from boto3.dynamodb.types import TypeDeserializer

TABLE_NAME = "jef-wallets-ledgers"

ddb = boto3.client("dynamodb")
deser = TypeDeserializer()

def ddb_item_to_python(item):
    return {k: deser.deserialize(v) for k, v in item.items()}

def get_all_items(table_name=TABLE_NAME):
    out = []
    start_key = None

    while True:
        req = {"TableName": table_name}
        if start_key:
            req["ExclusiveStartKey"] = start_key

        resp = ddb.scan(**req)

        for it in resp.get("Items", []):
            out.append(ddb_item_to_python(it))

        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break

    return out

def _json_default(o):
    if isinstance(o, Decimal):
        if o % 1 == 0:
            return int(o)
        return float(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

items = get_all_items()
print("count:", len(items))
print(json.dumps(items, indent=2, ensure_ascii=False, default=_json_default))

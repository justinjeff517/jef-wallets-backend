import json
import boto3
from decimal import Decimal
from boto3.dynamodb.types import TypeDeserializer
from botocore.config import Config

TABLE_NAME = "jef-wallets-ledgers"

ddb = boto3.client(
    "dynamodb",
    config=Config(retries={"max_attempts": 10, "mode": "standard"})
)
deser = TypeDeserializer()

def ddb_item_to_python(item):
    return {k: deser.deserialize(v) for k, v in item.items()}

def get_all_items(table_name=TABLE_NAME, page_limit=1000):
    out = []
    start_key = None

    while True:
        req = {
            "TableName": table_name,
            "Limit": page_limit,
        }
        if start_key:
            req["ExclusiveStartKey"] = start_key

        resp = ddb.scan(**req)

        for it in resp.get("Items", []) or []:
            out.append(ddb_item_to_python(it))

        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break

    return out

def _json_default(o):
    if isinstance(o, Decimal):
        # keep integers as int, otherwise float
        if o == o.to_integral_value():
            return int(o)
        return float(o)
    # safe fallback for unexpected types (rare)
    return str(o)

items = get_all_items()
print("count:", len(items))
print(json.dumps(items, indent=2, ensure_ascii=False, default=_json_default))

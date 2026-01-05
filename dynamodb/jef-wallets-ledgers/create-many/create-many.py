import json
import boto3
from decimal import Decimal

TABLE_NAME = "jef-wallets-ledgers"
FILE_PATH = r"H:\github12\jef-wallets-backend\dynamodb\jef-wallets-ledgers\create-many\datas.json"

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

def load_items(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f, parse_float=Decimal)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise ValueError("datas.json must be an object or array of objects")

def upload_items(items):
    ok = 0
    fail = 0
    errors = []
    with table.batch_writer(overwrite_by_pkeys=["pk", "sk"]) as batch:
        for i, item in enumerate(items):
            try:
                if not isinstance(item, dict):
                    raise ValueError("Item must be an object")
                if "pk" not in item or "sk" not in item:
                    raise ValueError("Missing pk/sk")
                batch.put_item(Item=item)
                ok += 1
            except Exception as e:
                fail += 1
                errors.append({"index": i, "error": str(e)})
    return {"uploaded": ok, "failed": fail, "errors": errors[:20]}

items = load_items(FILE_PATH)
result = upload_items(items)
print(result)

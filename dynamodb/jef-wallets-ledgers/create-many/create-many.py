import json
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError

TABLE_NAME = "jef-wallets-ledgers"
FILE_PATH = r"H:\github12\jef-wallets-backend\dynamodb\jef-wallets-ledgers\create-many\datas.json"

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

def load_items(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f, parse_float=Decimal, parse_int=Decimal)

    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise ValueError("datas.json must be an object or array of objects")

def validate_item(item):
    if not isinstance(item, dict):
        raise ValueError("Item must be an object")

    pk = item.get("pk")
    sk = item.get("sk")
    if not isinstance(pk, str) or not pk.strip():
        raise ValueError("Missing/invalid pk (must be non-empty string)")
    if not isinstance(sk, str) or not sk.strip():
        raise ValueError("Missing/invalid sk (must be non-empty string)")

    # Optional: ensure number fields are Decimal-friendly
    for k in ("balance_before", "amount", "balance_after"):
        if k in item and item[k] is not None:
            v = item[k]
            if not isinstance(v, (int, float, Decimal, str)):
                raise ValueError(f"Invalid {k} type: {type(v).__name__}")
            if isinstance(v, str):
                item[k] = Decimal(v)

    return item

def upload_items(items):
    ok = 0
    fail = 0
    errors = []

    # Fix: batch_writer overwrite keys must match your table's *actual* key schema.
    # If your table keys are (account_number, ledger_id), use those.
    # If your table keys are (pk, sk), keep (pk, sk).
    overwrite_keys = ["pk", "sk"]

    try:
        with table.batch_writer(overwrite_by_pkeys=overwrite_keys) as batch:
            for i, item in enumerate(items):
                try:
                    item = validate_item(item)
                    batch.put_item(Item=item)
                    ok += 1
                except Exception as e:
                    fail += 1
                    errors.append({"index": i, "error": str(e)})
    except ClientError as e:
        return {"uploaded": ok, "failed": fail + (len(items) - ok - fail), "errors": [{"error": str(e)}]}

    return {"uploaded": ok, "failed": fail, "errors": errors[:20]}

items = load_items(FILE_PATH)
result = upload_items(items)
print(json.dumps(result, indent=2, ensure_ascii=False, default=str))

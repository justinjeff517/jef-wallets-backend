import json
import time
import math
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError

TABLE_NAME = "jef-wallets-ledgers"
FILE_PATH = r"H:\github12\jef-wallets-backend\dynamodb\jef-wallets-ledgers\create-many\datas.json"

# Free tier (provisioned) baseline: 25 WCU
WCU_PER_SEC = 25
SAFETY = 0.9  # use 90% of limit

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
ddb = boto3.client("dynamodb")


def load_items(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f, parse_float=Decimal, parse_int=Decimal)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise ValueError("datas.json must be an object or array of objects")


def validate_item(item):
    pk = item.get("pk")
    if not isinstance(pk, str) or not pk.strip():
        raise ValueError("Missing/invalid pk")

    if item.get("ledger_id") is None:
        item["ledger_id"] = pk
    elif item["ledger_id"] != pk:
        raise ValueError("ledger_id must match pk")

    for k in ("account_number", "type", "description", "date", "created"):
        if not isinstance(item.get(k), str) or not item[k].strip():
            raise ValueError(f"Missing/empty {k}")

    if item["type"] not in ("credit", "debit"):
        raise ValueError("type must be credit|debit")

    for k in ("balance_before", "amount", "balance_after"):
        if k in item and item[k] is not None and not isinstance(item[k], Decimal):
            item[k] = Decimal(str(item[k]).strip())

    return item


def est_wcu(item):
    b = len(json.dumps(item, default=str, separators=(",", ":")).encode("utf-8"))
    return max(1, math.ceil(b / 1024))


def put_with_wcu_limit(items):
    ok, fail, errors = 0, 0, []
    limit = max(1, int(WCU_PER_SEC * SAFETY))

    window_start = time.monotonic()
    used = 0

    for i, raw in enumerate(items):
        try:
            it = validate_item(raw)
            need = est_wcu(it)

            if used + need > limit:
                sleep = 1.0 - (time.monotonic() - window_start)
                if sleep > 0:
                    time.sleep(sleep)
                window_start = time.monotonic()
                used = 0

            table.put_item(Item=it)
            used += need
            ok += 1

        except Exception as e:
            fail += 1
            errors.append({"index": i, "error": str(e)})

    return {"uploaded": ok, "failed": fail, "errors": errors[:20], "wcu_per_sec": WCU_PER_SEC}


items = load_items(FILE_PATH)
print(f"Loaded {len(items)} items")
res = put_with_wcu_limit(items)
print(json.dumps(res, indent=2, ensure_ascii=False, default=str))

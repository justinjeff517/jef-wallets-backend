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


def _as_str(v):
    return v.strip() if isinstance(v, str) else ""


def _to_decimal(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        return Decimal(s)
    return Decimal(str(v))


def validate_item(item):
    if not isinstance(item, dict):
        raise ValueError("Each item must be an object/dict")

    # pk required
    pk = _as_str(item.get("pk"))
    if not pk:
        raise ValueError("Missing/invalid pk")

    # ledger_id must match pk
    ledger_id = item.get("ledger_id")
    if ledger_id is None:
        item["ledger_id"] = pk
    else:
        ledger_id = _as_str(ledger_id)
        if not ledger_id:
            raise ValueError("Missing/invalid ledger_id")
        if ledger_id != pk:
            raise ValueError("ledger_id must match pk")

    # Required string fields (new schema)
    required_str = (
        "account_number",
        "sender_account_number",
        "sender_account_name",
        "receiver_account_number",
        "receiver_account_name",
        "date",
        "date_name",
        "created",
        "created_name",
        "created_by",
        "type",
        "description",
    )
    for k in required_str:
        if not _as_str(item.get(k)):
            raise ValueError(f"Missing/empty {k}")

    if item["type"] not in ("credit", "debit"):
        raise ValueError("type must be credit|debit")

    # amount required number
    item["amount"] = _to_decimal(item.get("amount"))
    if item["amount"] is None:
        raise ValueError("Missing/invalid amount")

    # Derive indexes (new schema)
    item["gsi_1_pk"] = _as_str(item["account_number"])
    item["gsi_1_sk"] = f'{_as_str(item["created"])}#{_as_str(item["ledger_id"])}'

    # Drop old fields if present (no longer in schema)
    for k in ("balance_before", "balance_after"):
        if k in item:
            item.pop(k, None)

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

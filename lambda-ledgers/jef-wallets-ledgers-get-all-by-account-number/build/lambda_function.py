import os
import json
import time
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key

AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
TABLE_NAME = (os.getenv("WALLETS_LEDGERS_TABLE") or "jef-wallets-ledgers").strip()
GSI_1_NAME = (os.getenv("WALLETS_LEDGERS_GSI_1_NAME") or "gsi_1").strip()

# DynamoDB provisioned baseline (Free tier): 25 RCU. Use 22 as requested.
RCU_PER_SEC = 22
SAFETY = 0.9  # use 90% of limit
_effective_rcu = max(1.0, RCU_PER_SEC * SAFETY)  # 19.8

_TZ_MANILA = timezone(timedelta(hours=8))

ddb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
table = ddb.Table(TABLE_NAME)

# ----------------------------
# HELPERS
# ----------------------------
def _as_str(v):
    return v.strip() if isinstance(v, str) else ""

def _to_jsonable(v):
    if isinstance(v, Decimal):
        return int(v) if v % 1 == 0 else float(v)
    if isinstance(v, dict):
        return {k: _to_jsonable(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_to_jsonable(x) for x in v]
    return v

def _parse_iso_dt(s: str):
    s = _as_str(s)
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_TZ_MANILA)
        return dt
    except Exception:
        return None

def _human_elapsed_hhmmss(created_iso: str) -> str:
    dt = _parse_iso_dt(created_iso)
    if not dt:
        return ""

    now = datetime.now(_TZ_MANILA)
    dt = dt.astimezone(_TZ_MANILA)

    delta = now - dt
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        total_seconds = 0

    h = total_seconds // 3600
    rem = total_seconds % 3600
    m = rem // 60
    s = rem % 60
    return f"{h:02d}:{m:02d}:{s:02d} ago"

def _pick_ledger_fields(it: dict) -> dict:
    created_iso = _as_str(it.get("created"))
    return {
        "account_number": _as_str(it.get("account_number")),
        "sender_account_number": _as_str(it.get("sender_account_number")),
        "sender_account_name": _as_str(it.get("sender_account_name")),
        "receiver_account_number": _as_str(it.get("receiver_account_number")),
        "receiver_account_name": _as_str(it.get("receiver_account_name")),
        "ledger_id": _as_str(it.get("ledger_id")),
        "date": _as_str(it.get("date")),
        "date_name": _as_str(it.get("date_name")),
        "created": created_iso,
        "created_name": _as_str(it.get("created_name")),
        "created_by": _as_str(it.get("created_by")),
        "type": _as_str(it.get("type")),
        "description": _as_str(it.get("description")),
        "amount": _to_jsonable(it.get("amount", 0)),
        "elapsed_time": _human_elapsed_hhmmss(created_iso),
    }

def _created_ts(s: str) -> float:
    dt = _parse_iso_dt(s)
    if not dt:
        return float("-inf")
    return dt.astimezone(_TZ_MANILA).timestamp()

def _sort_by_created_iso_desc(ledgers: list) -> list:
    return sorted(ledgers, key=lambda x: _created_ts(_as_str(x.get("created"))), reverse=True)

def _throttle_rcu(consumed_rcu: float):
    # Sleep long enough so average consumption stays under the configured RCU/sec.
    # For query, DynamoDB returns ConsumedCapacity.CapacityUnits.
    if consumed_rcu is None:
        return
    if consumed_rcu <= 0:
        return
    sleep_s = float(consumed_rcu) / float(_effective_rcu)
    if sleep_s > 0:
        time.sleep(sleep_s)

def get_ledgers_by_account_number(account_number: str, limit: int = 200) -> dict:
    account_number = _as_str(account_number)
    if not account_number:
        return {"exists": False, "message": "Missing account_number", "ledgers": []}

    items = []
    last_evaluated_key = None

    try:
        while True:
            page_limit = max(1, min(200, limit - len(items)))  # keep pages small and consistent
            if page_limit <= 0:
                break

            kwargs = {
                "IndexName": GSI_1_NAME,
                "KeyConditionExpression": Key("gsi_1_pk").eq(account_number),
                "ScanIndexForward": True,
                "Limit": page_limit,
                "ReturnConsumedCapacity": "TOTAL",
            }
            if last_evaluated_key:
                kwargs["ExclusiveStartKey"] = last_evaluated_key

            resp = table.query(**kwargs)

            cap = (resp.get("ConsumedCapacity") or {}).get("CapacityUnits")
            if cap is not None:
                _throttle_rcu(float(cap))

            page_items = resp.get("Items", []) or []
            items.extend(page_items)

            last_evaluated_key = resp.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

    except Exception as e:
        return {"exists": False, "message": f"Query failed: {str(e)}", "ledgers": []}

    items = _to_jsonable(items)
    ledgers = [_pick_ledger_fields(it) for it in items]
    ledgers = _sort_by_created_iso_desc(ledgers)

    exists = len(ledgers) > 0
    message = "Ledgers found" if exists else "No ledgers found"

    return {"exists": exists, "message": message, "ledgers": ledgers}

def lambda_handler(event, context=None):
    payload = event or {}

    if isinstance(payload, dict) and isinstance(payload.get("body"), str):
        try:
            payload = json.loads(payload["body"])
        except Exception:
            payload = {}

    account_number = _as_str(payload.get("account_number"))
    out = get_ledgers_by_account_number(account_number=account_number)

    if isinstance(event, dict) and "requestContext" in event:
        return {
            "statusCode": 200,
            "headers": {"content-type": "application/json"},
            "body": json.dumps(out, ensure_ascii=False),
        }

    return out

print(get_ledgers_by_account_number("1001"))

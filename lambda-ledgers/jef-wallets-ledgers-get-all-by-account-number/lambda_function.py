import os
import json
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key

# ----------------------------
# CONFIG
# ----------------------------
AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
TABLE_NAME = (os.getenv("WALLETS_LEDGERS_TABLE") or "jef-wallets-ledgers").strip()
GSI_1_NAME = (os.getenv("WALLETS_LEDGERS_GSI_1_NAME") or "gsi_1").strip()

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

def _fmt_hhmmss_from_seconds(total_seconds: int) -> str:
    if total_seconds < 0:
        total_seconds = 0
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def _elapsed_time(created_iso: str) -> str:
    dt = _parse_iso_dt(created_iso)
    if not dt:
        return ""

    dt_mnl = dt.astimezone(_TZ_MANILA)
    now_mnl = datetime.now(_TZ_MANILA)

    if dt_mnl.date() == now_mnl.date():
        delta = now_mnl - dt_mnl
        return _fmt_hhmmss_from_seconds(int(delta.total_seconds()))
    else:
        return dt_mnl.strftime("%m:%d")

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
        "balance_before": _to_jsonable(it.get("balance_before", 0)),
        "amount": _to_jsonable(it.get("amount", 0)),
        "balance_after": _to_jsonable(it.get("balance_after", 0)),
        "elapsed_time": _elapsed_time(created_iso),
    }

def _sort_by_created_iso(ledgers: list) -> list:
    def keyfn(x):
        s = _as_str(x.get("created"))
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        except Exception:
            return float("-inf")
    return sorted(ledgers, key=keyfn)

# ----------------------------
# CORE
# ----------------------------
def get_ledgers_by_account_number(account_number: str, limit: int = 200) -> dict:
    account_number = _as_str(account_number)
    if not account_number:
        return {"exists": False, "message": "Missing account_number", "ledgers": []}

    try:
        resp = table.query(
            IndexName=GSI_1_NAME,
            KeyConditionExpression=Key("gsi_1_pk").eq(account_number),
            ScanIndexForward=True,
            Limit=limit,
        )
        items = resp.get("Items", []) or []
    except Exception as e:
        return {"exists": False, "message": f"Query failed: {str(e)}", "ledgers": []}

    items = _to_jsonable(items)
    ledgers = [_pick_ledger_fields(it) for it in items]
    ledgers = _sort_by_created_iso(ledgers)

    exists = len(ledgers) > 0
    message = "Ledgers found" if exists else "No ledgers found"

    return {"exists": exists, "message": message, "ledgers": ledgers}

# ----------------------------
# LAMBDA HANDLER
# ----------------------------
def lambda_handler(event, context=None):
    payload = event or {}

    # API Gateway / Lambda proxy support (body is JSON string)
    if isinstance(payload, dict) and isinstance(payload.get("body"), str):
        try:
            payload = json.loads(payload["body"])
        except Exception:
            payload = {}

    account_number = _as_str(payload.get("account_number"))
    out = get_ledgers_by_account_number(account_number=account_number)

    # Proxy response if behind API Gateway
    if isinstance(event, dict) and "requestContext" in event:
        return {
            "statusCode": 200,
            "headers": {"content-type": "application/json"},
            "body": json.dumps(out, ensure_ascii=False),
        }

    return out

# ----------------------------
# LOCAL TEST
# ----------------------------
if __name__ == "__main__":
    print(get_ledgers_by_account_number("1001"))

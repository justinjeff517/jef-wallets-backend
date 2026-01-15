import os
import json
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key

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

def _human_age(created_iso: str) -> str:
    dt = _parse_iso_dt(created_iso)
    if not dt:
        return ""

    now = datetime.now(_TZ_MANILA)
    dt = dt.astimezone(_TZ_MANILA)

    delta = now - dt
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        total_seconds = 0

    # seconds / minutes / hours
    if total_seconds < 60:
        return f"{total_seconds} s ago"
    if total_seconds < 3600:
        m = total_seconds // 60
        s = total_seconds % 60
        if s == 0:
            return f"{m}m ago"
        return f"{m}m {s} s ago"
    if total_seconds < 86400:
        h = total_seconds // 3600
        rem = total_seconds % 3600
        m = rem // 60
        if m == 0:
            return f"{h} hours ago" if h != 1 else "1 hour ago"
        return f"{h} hours {m}m ago" if h != 1 else f"1 hour {m}m ago"

    # days
    days = total_seconds // 86400
    if days < 30:
        return f"{days} days ago" if days != 1 else "1 day ago"

    # months + days (approx: 30d/month)
    months = days // 30
    rem_days = days % 30
    if rem_days == 0:
        return f"{months} months ago" if months != 1 else "1 month ago"
    return f"{months} months {rem_days} days ago" if months != 1 else f"1 month {rem_days} days ago"

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
        "elapsed_time": _human_age(created_iso),
    }

def _sort_by_created_iso(ledgers: list) -> list:
    def keyfn(x):
        s = _as_str(x.get("created"))
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        except Exception:
            return float("-inf")
    return sorted(ledgers, key=keyfn)

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

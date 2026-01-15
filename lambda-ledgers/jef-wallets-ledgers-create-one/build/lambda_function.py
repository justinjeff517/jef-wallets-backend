import os
import json
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# ----------------------------
# CONFIG
# ----------------------------
AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
TABLE_NAME = (os.getenv("WALLETS_LEDGERS_TABLE") or "jef-wallets-ledgers").strip()

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

def _to_decimal(v):
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        return Decimal(s)
    return None

def _fmt_date_name(dt):
    return dt.strftime("%B, %-d, %Y, %A") if os.name != "nt" else dt.strftime("%B, %#d, %Y, %A")

def _fmt_created_name(dt):
    day = dt.strftime("%-d") if os.name != "nt" else dt.strftime("%#d")
    return dt.strftime(f"%B, {day}, %Y, %A, %-I:%M %p") if os.name != "nt" else dt.strftime(f"%B, {day}, %Y, %A, %#I:%M %p")

def _jsonable(v):
    if isinstance(v, Decimal):
        return float(v)
    return v

def _safe_json_loads(s):
    try:
        return json.loads(s) if isinstance(s, str) and s.strip() else None
    except Exception:
        return None

# ----------------------------
# CORE
# ----------------------------
def create_one_ledger(payload: dict):
    p = payload or {}

    account_number = _as_str(p.get("account_number"))
    sender_account_number = _as_str(p.get("sender_account_number"))
    sender_account_name = _as_str(p.get("sender_account_name"))
    receiver_account_number = _as_str(p.get("receiver_account_number"))
    receiver_account_name = _as_str(p.get("receiver_account_name"))
    typ = _as_str(p.get("type")).lower()
    description = _as_str(p.get("description"))
    created_by = _as_str(p.get("created_by"))
    ledger_id = _as_str(p.get("ledger_id"))
    amount = _to_decimal(p.get("amount"))

    if not account_number:
        return {"is_created": False, "message": "account_number is required", "ledger_id": ""}
    if not ledger_id:
        return {"is_created": False, "message": "ledger_id is required", "ledger_id": ""}
    if typ not in ("credit", "debit"):
        return {"is_created": False, "message": "type must be 'credit' or 'debit'", "ledger_id": ledger_id}
    if amount is None or amount <= 0:
        return {"is_created": False, "message": "amount must be a positive number", "ledger_id": ledger_id}

    now = datetime.now(_TZ_MANILA)
    created_iso = now.isoformat(timespec="seconds")
    date_str = now.strftime("%Y-%m-%d")
    date_name = _fmt_date_name(now)
    created_name = _fmt_created_name(now)

    item = {
        "pk": ledger_id,
        "gsi_1_pk": account_number,
        "gsi_1_sk": f"{created_iso}#{ledger_id}",
        "account_number": account_number,
        "sender_account_number": sender_account_number or account_number,
        "sender_account_name": sender_account_name,
        "receiver_account_number": receiver_account_number,
        "receiver_account_name": receiver_account_name,
        "ledger_id": ledger_id,
        "date": date_str,
        "date_name": date_name,
        "created": created_iso,
        "created_name": created_name,
        "created_by": created_by,
        "type": typ,
        "description": description,
        "amount": amount,
    }

    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(pk)",
        )
        return {"is_created": True, "message": "Created", "ledger_id": ledger_id}
    except ClientError as e:
        code = (e.response or {}).get("Error", {}).get("Code", "")
        if code == "ConditionalCheckFailedException":
            return {"is_created": False, "message": "ledger_id already exists", "ledger_id": ledger_id}
        msg = (e.response or {}).get("Error", {}).get("Message", "")
        return {"is_created": False, "message": f"DynamoDB error: {code} {msg}".strip(), "ledger_id": ledger_id}
    except Exception as e:
        return {"is_created": False, "message": f"Unexpected error: {str(e)}", "ledger_id": ledger_id}

# ----------------------------
# SQS LAMBDA HANDLER
# ----------------------------
def lambda_handler(event, context):
    records = (event or {}).get("Records") or []
    results = []

    for r in records:
        body_raw = r.get("body", "")
        payload = _safe_json_loads(body_raw)

        if not isinstance(payload, dict):
            results.append({"is_created": False, "message": "Invalid JSON body", "ledger_id": ""})
            continue

        res = create_one_ledger(payload)
        results.append(res)

        if not res.get("is_created"):
            raise Exception(json.dumps({"message": "Failed to create ledger", "result": res}, default=_jsonable))

    return {
        "ok": True,
        "processed": len(records),
        "results": results,
    }

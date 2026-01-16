import os
import json
import uuid
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

# -----------------------------
# CONFIG
# -----------------------------
AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
TABLE_NAME = (os.getenv("WALLETS_TRANSACTIONS_TABLE") or "jef-wallets-transactions").strip()

# Asia/Manila is UTC+8 (fixed offset, no zoneinfo)
PH_TZ = timezone(timedelta(hours=8))

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(TABLE_NAME)

# -----------------------------
# HELPERS
# -----------------------------
def _as_str(v):
    return v.strip() if isinstance(v, str) else ""

def _to_dec(v):
    if isinstance(v, Decimal):
        return v
    if isinstance(v, int):
        return Decimal(v)
    if isinstance(v, float):
        return Decimal(str(v))
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return Decimal("0")
        return Decimal(s)
    return Decimal("0")

def _month_name(m):
    names = [
        "January","February","March","April","May","June",
        "July","August","September","October","November","December"
    ]
    return names[m - 1]

def _weekday_name(dt):
    names = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    return names[dt.weekday()]

def _format_date_name(dt):
    # "Month, Day, Year, Weekday" (per your spec)
    return f"{_month_name(dt.month)} {dt.day}, {dt.year}, {_weekday_name(dt)}"

def _format_created_name(dt):
    # "Month, Day, Year, Weekday, Hour Minute AM/PM"
    hour = dt.hour
    ampm = "AM" if hour < 12 else "PM"
    h12 = hour % 12
    if h12 == 0:
        h12 = 12
    minute = f"{dt.minute:02d}"
    return f"{_month_name(dt.month)} {dt.day}, {dt.year}, {_weekday_name(dt)}, {h12}:{minute} {ampm}"

def _validate_payload(p):
    required = [
        "account_number",
        "sender_account_number",
        "sender_account_name",
        "receiver_account_number",
        "receiver_account_name",
        "description",
        "amount",
        "transaction_id",
        "created_by",
    ]
    for k in required:
        if k not in p:
            return False, f"missing field: {k}"
        if k != "amount" and _as_str(p[k]) == "":
            return False, f"empty field: {k}"

    amt = _to_dec(p["amount"])
    if amt <= 0:
        return False, "amount must be > 0"

    tid = _as_str(p["transaction_id"])
    try:
        uuid.UUID(tid)
    except Exception:
        return False, "transaction_id must be uuidv4 format"

    return True, "ok"

# -----------------------------
# MAIN ACTION
# -----------------------------
def create_transaction(payload):
    ok, msg = _validate_payload(payload)
    if not ok:
        return {"is_created": False, "message": msg}

    now = datetime.now(PH_TZ)
    created_iso = now.isoformat(timespec="seconds")
    date_str = now.date().isoformat()

    item = {
        # main pk per your schema
        "pk": _as_str(payload["transaction_id"]),
        "transaction_id": _as_str(payload["transaction_id"]),
        "account_number": _as_str(payload["account_number"]),
        "sender_account_number": _as_str(payload["sender_account_number"]),
        "sender_account_name": _as_str(payload["sender_account_name"]),
        "receiver_account_number": _as_str(payload["receiver_account_number"]),
        "receiver_account_name": _as_str(payload["receiver_account_name"]),
        "description": _as_str(payload["description"]),
        "amount": _to_dec(payload["amount"]),
        "created_by": _as_str(payload["created_by"]),
        # keep type if you still want it stored; if not needed, remove this line
        "type": "sender" if _as_str(payload["account_number"]) == _as_str(payload["sender_account_number"]) else "receiver",
        "date": date_str,
        "date_name": _format_date_name(now),
        "created": created_iso,
        "created_name": _format_created_name(now),

        # GSIs (sender and receiver timelines)
        "gsi_1_pk": _as_str(payload["sender_account_number"]),
        "gsi_1_sk": created_iso,
        "gsi_2_pk": _as_str(payload["receiver_account_number"]),
        "gsi_2_sk": created_iso,
    }

    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(pk)"
        )
        return {"is_created": True, "message": "created"}
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "ConditionalCheckFailedException":
            return {"is_created": False, "message": "transaction_id already exists"}
        return {"is_created": False, "message": f"dynamodb error: {code}"}
    except Exception as e:
        return {"is_created": False, "message": f"error: {str(e)}"}

# -----------------------------
# EXAMPLE USAGE (creates 1 item)
# -----------------------------
payload = {
    "account_number": "1001",
    "sender_account_number": "1001",
    "sender_account_name": "Ellorimo Farm",
    "receiver_account_number": "1002",
    "receiver_account_name": "JEF Eggstore",
    "description": "Daily egg sales remittance (afternoon batch)",
    "amount": 6890,
    "transaction_id": str(uuid.uuid4()),
    "created_by": "00031",
}

res = create_transaction(payload)
print(json.dumps(res, indent=2))

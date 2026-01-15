import os
import json
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, timedelta
import time

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
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return Decimal(s)
        except (InvalidOperation, ValueError):
            return None
    return None

def _fmt_date_name(dt):
    day = dt.day
    return f"{dt.strftime('%B')}, {day}, {dt.year}, {dt.strftime('%A')}"

def _fmt_created_name(dt):
    day = dt.day
    time_str = dt.strftime("%I:%M %p").lstrip("0")
    return f"{dt.strftime('%B')}, {day}, {dt.year}, {dt.strftime('%A')}, {time_str}"

# ----------------------------
# MAIN
# ----------------------------
def create_one_ledger(payload: dict):
    """
    New schema (no balances, no STATE item):
      pk         = <ledger_id>
      gsi_1_pk    = <account_number>
      gsi_1_sk    = <created>#<ledger_id>
    """
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

    # enforce: account_number == sender_account_number (your stated rule)
    if sender_account_number and sender_account_number != account_number:
        return {"is_created": False, "message": "account_number must match sender_account_number", "ledger_id": ledger_id}
    if not sender_account_number:
        sender_account_number = account_number

    now = datetime.now(_TZ_MANILA)
    created_iso = now.isoformat(timespec="seconds")
    date_str = now.strftime("%Y-%m-%d")
    date_name = _fmt_date_name(now)
    created_name = _fmt_created_name(now)

    ledger_item = {
        "pk": ledger_id,
        "gsi_1_pk": account_number,
        "gsi_1_sk": f"{created_iso}#{ledger_id}",
        "account_number": account_number,
        "sender_account_number": sender_account_number,
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

    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            table.put_item(
                Item=ledger_item,
                ConditionExpression="attribute_not_exists(pk)",
            )

            return {
                "is_created": True,
                "message": "Created",
                "ledger_id": ledger_id,
            }

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            msg = e.response.get("Error", {}).get("Message", "")

            if code == "ConditionalCheckFailedException":
                return {
                    "is_created": False,
                    "message": "ledger_id already exists",
                    "ledger_id": ledger_id,
                }

            if code in ("ProvisionedThroughputExceededException", "ThrottlingException", "RequestLimitExceeded"):
                if attempt < max_attempts:
                    time.sleep(0.05 * (2 ** (attempt - 1)))
                    continue
                return {
                    "is_created": False,
                    "message": f"Throttled: {code}: {msg}",
                    "ledger_id": ledger_id,
                }

            return {
                "is_created": False,
                "message": f"DynamoDB error: {code}: {msg}",
                "ledger_id": ledger_id,
            }

        except Exception as e:
            return {
                "is_created": False,
                "message": f"Unexpected error: {str(e)}",
                "ledger_id": ledger_id,
            }

    return {
        "is_created": False,
        "message": "Max retry attempts exceeded",
        "ledger_id": ledger_id,
    }


if __name__ == "__main__":
    example_payload = {
        "account_number": "1001",
        "sender_account_number": "1001",
        "sender_account_name": "JEF Eggstore",
        "receiver_account_number": "2001",
        "receiver_account_name": "Supplier A",
        "type": "debit",
        "description": "Purchase of packaging materials",
        "amount": 1250.50,
        "created_by": "00001",
        "ledger_id": "9e3d7b6c-3b2c-4a53-9f2e-3b0d5c2d8b44",
    }

    resp = create_one_ledger(example_payload)
    print(json.dumps(resp, indent=2, default=str))

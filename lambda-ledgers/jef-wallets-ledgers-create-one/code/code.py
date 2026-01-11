import os
import json
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

LEDGERS_TABLE = os.getenv("LEDGERS_TABLE") or "jef-wallets-ledgers"
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"

_ddb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
_table = _ddb.Table(LEDGERS_TABLE)

_TZ = timezone(timedelta(hours=8))  # Asia/Manila fixed offset


def _as_str(v):
    return v.strip() if isinstance(v, str) else ""


def _as_num(v):
    if isinstance(v, (int, float, Decimal)):
        return Decimal(str(v))
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        return Decimal(s)
    return None


def _iso_now():
    return datetime.now(_TZ).isoformat(timespec="seconds")


def _date_ymd(iso_ts):
    return iso_ts[:10]


def _date_name(iso_ts):
    # "January 2, 2026, Friday"
    dt = datetime.fromisoformat(iso_ts)
    return f"{dt.strftime('%B')} {dt.day}, {dt.year}, {dt.strftime('%A')}"


def _created_name(iso_ts):
    # "January 2, 2026, Friday, 9:05 AM"
    dt = datetime.fromisoformat(iso_ts)
    hour = dt.strftime("%I").lstrip("0") or "0"
    minute = dt.strftime("%M")
    ampm = dt.strftime("%p")
    return f"{dt.strftime('%B')} {dt.day}, {dt.year}, {dt.strftime('%A')}, {hour}:{minute} {ampm}"


def create_one_ledger(payload, *, enforce_idempotency=True):
    """
    Payload format:
    {
      "account_number": "string",
      "sender_account_number":"string",
      "sender_account_name":"string",
      "receiver_account_number":"string",
      "receiver_account_name":"string",
      "type":"credit|debit",
      "description":"string",
      "amount":"number",
      "created_by":"string",
      "ledger_id":"string-uuidv4"
    }

    Response:
    {"is_created": bool, "message": str}
    """
    try:
        account_number = _as_str(payload.get("account_number"))
        sender_account_number = _as_str(payload.get("sender_account_number"))
        sender_account_name = _as_str(payload.get("sender_account_name"))
        receiver_account_number = _as_str(payload.get("receiver_account_number"))
        receiver_account_name = _as_str(payload.get("receiver_account_name"))
        typ = _as_str(payload.get("type")).lower()
        description = _as_str(payload.get("description"))
        created_by = _as_str(payload.get("created_by"))
        ledger_id = _as_str(payload.get("ledger_id"))
        amount = _as_num(payload.get("amount"))

        if not account_number:
            return {"is_created": False, "message": "account_number is required"}
        if not ledger_id:
            return {"is_created": False, "message": "ledger_id is required"}
        if typ not in ("credit", "debit"):
            return {"is_created": False, "message": "type must be credit or debit"}
        if amount is None:
            return {"is_created": False, "message": "amount is required and must be a number"}
        if amount <= 0:
            return {"is_created": False, "message": "amount must be > 0"}
        if not created_by:
            return {"is_created": False, "message": "created_by is required"}

        created = _iso_now()
        date = _date_ymd(created)
        date_name = _date_name(created)
        created_name = _created_name(created)

        # NOTE: balance_before/balance_after are not provided in your payload.
        # This write will store amount and metadata; you can later compute balances
        # or extend this function to update balances atomically.
        item = {
            "pk": account_number,     # main index pk <account_number>
            "sk": ledger_id,          # main index sk <ledger_id>

            "account_number": account_number,
            "sender_account_number": sender_account_number,
            "sender_account_name": sender_account_name,
            "receiver_account_number": receiver_account_number,
            "receiver_account_name": receiver_account_name,

            "ledger_id": ledger_id,
            "date": date,
            "date_name": date_name,
            "created": created,
            "created_name": created_name,
            "created_by": created_by,

            "type": typ,
            "description": description,
            "amount": amount,
        }

        put_kwargs = {"Item": item}

        if enforce_idempotency:
            # prevent overwriting an existing ledger with same pk+sk
            put_kwargs["ConditionExpression"] = "attribute_not_exists(pk) AND attribute_not_exists(sk)"

        _table.put_item(**put_kwargs)

        return {"is_created": True, "message": "Ledger created"}

    except ClientError as e:
        code = (e.response.get("Error") or {}).get("Code") or ""
        if code == "ConditionalCheckFailedException":
            return {"is_created": False, "message": "ledger_id already exists for this account_number"}
        return {"is_created": False, "message": f"DynamoDB error: {code}"}
    except Exception as e:
        return {"is_created": False, "message": f"Unhandled error: {type(e).__name__}: {e}"}


# ---- example usage ----
if __name__ == "__main__":
    sample_payload = {
  "account_number": "1001",
  "sender_account_number": "1002",
  "sender_account_name": "JEF Eggstore",
  "receiver_account_number": "1001",
  "receiver_account_name": "Ellorimo Farm",
  "type": "credit",
  "description": "Egg sales remittance (daily cash drop) - PM batch",
  "amount": 1800,
  "created_by": "00031",
  "ledger_id": "66f906c3-1dd9-40dd-bd83-b11f6d98c676"
    }

    resp = create_one_ledger(sample_payload)
    print(json.dumps(resp, indent=2))

import os
import json
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
TABLE_NAME = (os.getenv("WALLETS_LEDGERS_TABLE") or "jef-wallets-ledgers").strip()

_TZ_MANILA = timezone(timedelta(hours=8))

ddb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
table = ddb.Table(TABLE_NAME)


def _as_str(v):
    return v.strip() if isinstance(v, str) else ""


def _to_decimal(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int, float)):
        try:
            return Decimal(str(v))
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        try:
            return Decimal(s)
        except Exception:
            return None
    return None


def _date_name(dt_obj):
    return f"{dt_obj:%B} {dt_obj.day}, {dt_obj:%Y}, {dt_obj:%A}"


def _created_name(dt_obj):
    # "January 2, 2026, Friday, 11:30 AM"
    return f"{dt_obj:%B} {dt_obj.day}, {dt_obj:%Y}, {dt_obj:%A}, {dt_obj:%I:%M %p}".lstrip("0").replace(", 0", ", ")


def _normalize_payload(p):
    payload = p if isinstance(p, dict) else {}
    account_number = _as_str(payload.get("account_number"))
    sender_account_number = _as_str(payload.get("sender_account_number"))
    sender_account_name = _as_str(payload.get("sender_account_name"))
    receiver_account_number = _as_str(payload.get("receiver_account_number"))
    receiver_account_name = _as_str(payload.get("receiver_account_name"))
    typ = _as_str(payload.get("type"))
    description = _as_str(payload.get("description"))
    created_by = _as_str(payload.get("created_by"))
    ledger_id = _as_str(payload.get("ledger_id"))
    amount = _to_decimal(payload.get("amount"))

    if not ledger_id:
        return None, "Missing ledger_id"
    if not account_number:
        return None, "Missing account_number"
    if not sender_account_number:
        return None, "Missing sender_account_number"
    if not receiver_account_number:
        return None, "Missing receiver_account_number"
    if typ not in ("credit", "debit"):
        return None, "type must be 'credit' or 'debit'"
    if amount is None:
        return None, "Invalid amount"
    if not created_by:
        return None, "Missing created_by"

    now = datetime.now(_TZ_MANILA).replace(microsecond=0)
    created_iso = now.isoformat()

    item = {
        "pk": ledger_id,
        "gsi_1_pk": sender_account_number,
        "gsi_1_sk": f"{created_iso}#{ledger_id}",
        "gsi_2_pk": receiver_account_number,
        "gsi_2_sk": f"{created_iso}#{ledger_id}",
        "account_number": account_number,
        "sender_account_number": sender_account_number,
        "sender_account_name": sender_account_name,
        "receiver_account_number": receiver_account_number,
        "receiver_account_name": receiver_account_name,
        "ledger_id": ledger_id,
        "date": created_iso[:10],
        "date_name": _date_name(now),
        "created": created_iso,
        "created_name": _created_name(now),
        "created_by": created_by,
        "type": typ,
        "description": description,
        "amount": amount,
    }

    return item, ""


def _put_item(item):
    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(pk)",
    )
    return {"is_created": True, "message": "Created", "ledger_id": item.get("ledger_id", "")}


def lambda_handler(event, context):
    results = []
    records = event.get("Records") or []

    for r in records:
        msg_id = _as_str(r.get("messageId"))
        body_raw = r.get("body")

        try:
            if isinstance(body_raw, (bytes, bytearray)):
                body_raw = body_raw.decode("utf-8", errors="replace")

            payload = json.loads(body_raw) if isinstance(body_raw, str) else (body_raw if isinstance(body_raw, dict) else {})
        except Exception as e:
            results.append(
                {
                    "messageId": msg_id,
                    "ok": False,
                    "result": {"is_created": False, "message": f"Invalid JSON body: {e}", "ledger_id": ""},
                }
            )
            continue

        item, err = _normalize_payload(payload)
        if item is None:
            results.append(
                {
                    "messageId": msg_id,
                    "ok": False,
                    "result": {"is_created": False, "message": err, "ledger_id": _as_str(payload.get("ledger_id"))},
                }
            )
            continue

        try:
            res = _put_item(item)
            results.append({"messageId": msg_id, "ok": True, "result": res})

        except ClientError as e:
            code = (e.response.get("Error") or {}).get("Code", "")
            msg = (e.response.get("Error") or {}).get("Message", str(e))

            if code == "ConditionalCheckFailedException":
                # idempotent duplicate -> treat as success (already created)
                results.append(
                    {
                        "messageId": msg_id,
                        "ok": True,
                        "result": {"is_created": True, "message": "Already exists", "ledger_id": item.get("ledger_id", "")},
                    }
                )
                continue

            # For SQS triggers: raising causes retry + DLQ. Do that for transient/unknown errors.
            raise

        except Exception:
            raise

    # Helpful return for logs/testing; SQS trigger ignores response for success/failure.
    return {"ok": True, "results": results}

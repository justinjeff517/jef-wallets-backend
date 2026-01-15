import os
import json
from decimal import Decimal
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
SQS_URL = (
    os.getenv("WALLETS_LEDGERS_SQS_CREATE_ONE_URL")
    or "https://sqs.ap-southeast-1.amazonaws.com/246715082475/jef-wallets-ledgers-sqs-create-one"
).strip()

sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)

def _as_str(v):
    return v.strip() if isinstance(v, str) else ""

def _to_number(v):
    if isinstance(v, (int, float, Decimal)):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        try:
            return Decimal(s)
        except Exception:
            return None
    return None

def _jsonable(v):
    if isinstance(v, Decimal):
        return float(v)
    return v

def send_create_ledger_message(payload: dict):
    p = payload or {}

    msg = {
        "account_number": _as_str(p.get("account_number")),
        "sender_account_number": _as_str(p.get("sender_account_number")),
        "sender_account_name": _as_str(p.get("sender_account_name")),
        "receiver_account_number": _as_str(p.get("receiver_account_number")),
        "receiver_account_name": _as_str(p.get("receiver_account_name")),
        "type": _as_str(p.get("type")),
        "description": _as_str(p.get("description")),
        "amount": _to_number(p.get("amount")),
        "created_by": _as_str(p.get("created_by")),
        "ledger_id": _as_str(p.get("ledger_id")),
    }

    missing = [k for k, v in msg.items() if v in (None, "")]
    if missing:
        return {"is_sent": False, "message": f"Missing/invalid fields: {', '.join(missing)}"}

    if msg["type"] not in ("credit", "debit"):
        return {"is_sent": False, "message": "Invalid type. Must be 'credit' or 'debit'."}

    body = json.dumps(msg, default=_jsonable, separators=(",", ":"))

    try:
        resp = sqs.send_message(
            QueueUrl=SQS_URL,
            MessageBody=body,
        )
        message_id = resp.get("MessageId", "")
        return {"is_sent": True, "message": f"Sent. MessageId={message_id}"}
    except ClientError as e:
        return {"is_sent": False, "message": f"AWS error: {e.response.get('Error', {}).get('Message', str(e))}"}
    except Exception as e:
        return {"is_sent": False, "message": f"Error: {str(e)}"}

# Example:
payload = {
    "account_number": "1001",
    "sender_account_number": "1001",
    "sender_account_name": "JEF Main",
    "receiver_account_number": "2001",
    "receiver_account_name": "Supplier A",     
    "type": "debit",
    "description": "Payment for feed",
    "amount": 1250.75,
    "created_by": "00001",
    "ledger_id": "b6c4a6a8-0b2a-4dbe-8f66-8c8f8c8f8c8f",
}


print(send_create_ledger_message(payload))

import os
from uuid import uuid4
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeSerializer

# ----------------------------
# CONFIG
# ----------------------------
AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
TABLE_NAME = (os.getenv("WALLETS_LEDGERS_TABLE") or "jef-wallets-ledgers").strip()

# DynamoDB GSI names (set these env vars if your actual index names differ)
GSI3_NAME = (os.getenv("WALLETS_LEDGERS_GSI3_NAME") or "gsi_3").strip()

_TZ_MANILA = timezone(timedelta(hours=8))

ddb = boto3.client(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)

_ser = TypeSerializer()

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
    if isinstance(v, (int, float)):
        try:
            return Decimal(str(v))
        except Exception:
            return None
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
    return f"{dt.strftime('%B')} {dt.day}, {dt.year}, {dt.strftime('%A')}"

def _fmt_created_name(dt):
    hour12 = dt.strftime("%I").lstrip("0") or "12"
    minute = dt.strftime("%M")
    ampm = dt.strftime("%p")
    return f"{dt.strftime('%B')} {dt.day}, {dt.year}, {dt.strftime('%A')}, {hour12}:{minute} {ampm}"

def _serialize_item(py_item):
    clean = {k: v for k, v in py_item.items() if v is not None}
    return {k: _ser.serialize(v) for k, v in clean.items()}

def _transaction_exists(transaction_id):
    # Query GSI_3 by transaction_id (partition key)
    try:
        resp = ddb.query(
            TableName=TABLE_NAME,
            IndexName=GSI3_NAME,
            KeyConditionExpression="#p = :p",
            ExpressionAttributeNames={"#p": "gsi_3_pk"},
            ExpressionAttributeValues={":p": {"S": transaction_id}},
            ProjectionExpression="pk",
            Limit=1,
        )
        return (resp.get("Count") or 0) > 0
    except ClientError:
        # If index name is wrong or access denied, treat as "unknown" and allow create
        return False

# ----------------------------
# MAIN
# ----------------------------
def create_double_entry(payload, enforce_unique_transaction=True):
    """
    payload format:
    {
      "creator_account_number": "string",
      "sender_account_number": "string",
      "sender_account_name": "string",
      "receiver_account_number": "string",
      "receiver_account_name": "string",
      "description": "string",
      "amount": "number",
      "created_by": "string",
      "transaction_id": "string-uuidv4"
    }

    response format:
    {
      "is_created": bool,
      "message": str,
      "ledger_id": str
    }
    """
    creator_account_number = _as_str(payload.get("creator_account_number"))
    sender_account_number = _as_str(payload.get("sender_account_number"))
    sender_account_name = _as_str(payload.get("sender_account_name"))
    receiver_account_number = _as_str(payload.get("receiver_account_number"))
    receiver_account_name = _as_str(payload.get("receiver_account_name"))
    description = _as_str(payload.get("description"))
    created_by = _as_str(payload.get("created_by"))
    transaction_id = _as_str(payload.get("transaction_id"))
    amount = _to_decimal(payload.get("amount"))

    if not creator_account_number:
        return {"is_created": False, "message": "Missing creator_account_number", "ledger_id": ""}

    if not sender_account_number:
        return {"is_created": False, "message": "Missing sender_account_number", "ledger_id": ""}

    if not receiver_account_number:
        return {"is_created": False, "message": "Missing receiver_account_number", "ledger_id": ""}

    if not transaction_id:
        return {"is_created": False, "message": "Missing transaction_id", "ledger_id": ""}

    if amount is None or amount <= 0:
        return {"is_created": False, "message": "Invalid amount (must be > 0)", "ledger_id": ""}

    # Your rule: debit item only if creator_account_number is sender_account_number
    if creator_account_number != sender_account_number:
        return {
            "is_created": False,
            "message": "Rejected: creator_account_number must equal sender_account_number for debit creation",
            "ledger_id": "",
        }

    if created_by and not (len(created_by) == 5 and created_by.isdigit()):
        return {"is_created": False, "message": "Invalid created_by (must be 5 digits)", "ledger_id": ""}

    if enforce_unique_transaction and _transaction_exists(transaction_id):
        return {"is_created": False, "message": "Transaction already exists (transaction_id)", "ledger_id": ""}

    now = datetime.now(_TZ_MANILA)
    created = now.isoformat(timespec="seconds")
    date = now.date().isoformat()
    date_name = _fmt_date_name(now)
    created_name = _fmt_created_name(now)

    debit_ledger_id = str(uuid4())
    credit_ledger_id = str(uuid4())

    # Debit item (indexed for sender via GSI_1)
    debit_item = {
        "pk": debit_ledger_id,
        "ledger_id": debit_ledger_id,
        "transaction_id": transaction_id,
        "gsi_3_pk": transaction_id,
        "gsi_3_sk": "debit",
        "gsi_1_pk": sender_account_number,
        "gsi_1_sk": f"{created}#{debit_ledger_id}",
        "creator_account_number": creator_account_number,
        "sender_account_number": sender_account_number,
        "sender_account_name": sender_account_name,
        "receiver_account_number": receiver_account_number,
        "receiver_account_name": receiver_account_name,
        "date": date,
        "date_name": date_name,
        "created": created,
        "created_name": created_name,
        "created_by": created_by,
        "type": "debit",
        "description": description,
        "amount": amount,
    }

    # Credit item (indexed for receiver via GSI_2)
    credit_item = {
        "pk": credit_ledger_id,
        "ledger_id": credit_ledger_id,
        "transaction_id": transaction_id,
        "gsi_3_pk": transaction_id,
        "gsi_3_sk": "credit",
        "gsi_2_pk": receiver_account_number,
        "gsi_2_sk": f"{created}#{credit_ledger_id}",
        "creator_account_number": creator_account_number,
        "sender_account_number": sender_account_number,
        "sender_account_name": sender_account_name,
        "receiver_account_number": receiver_account_number,
        "receiver_account_name": receiver_account_name,
        "date": date,
        "date_name": date_name,
        "created": created,
        "created_name": created_name,
        "created_by": created_by,
        "type": "credit",
        "description": description,
        "amount": amount,
    }

    try:
        ddb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": TABLE_NAME,
                        "Item": _serialize_item(debit_item),
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }
                },
                {
                    "Put": {
                        "TableName": TABLE_NAME,
                        "Item": _serialize_item(credit_item),
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }
                },
            ]
        )

        return {
            "is_created": True,
            "message": f"Created debit={debit_ledger_id} and credit={credit_ledger_id} for transaction_id={transaction_id}",
            "ledger_id": debit_ledger_id,
        }

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return {"is_created": False, "message": f"DynamoDB error: {msg}", "ledger_id": ""}

# ----------------------------
# EXAMPLE (optional)
# ----------------------------
# payload = {
#     "creator_account_number": "1006",
#     "sender_account_number": "1006",
#     "sender_account_name": "JEF Minimart",
#     "receiver_account_number": "1010",
#     "receiver_account_name": "Internal - Caferimo Coffee Shop - Loboc",
#     "description": "Test transfer",
#     "amount": 123.45,
#     "created_by": "00001",
#     "transaction_id": str(uuid4()),
# }
# print(create_double_entry(payload))

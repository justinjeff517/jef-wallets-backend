import os
import json
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

_TZ_MANILA = timezone(timedelta(hours=8))

ddb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
table = ddb.Table(TABLE_NAME)
ddb_client = table.meta.client
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

def _ddb(v):
    return _ser.serialize(v)

def _state_pk(account_number: str) -> str:
    return f"STATE#{account_number}"

# ----------------------------
# MAIN
# ----------------------------
def create_one_ledger(payload: dict):
    """
    Computes balance_before/balance_after at write-time using an account STATE item in the SAME table.
    Atomicity is enforced via DynamoDB TransactWriteItems.

    STATE item shape (stored in same table):
    {
      "pk": "STATE#<account_number>",
      "kind": "account_state",
      "account_number": "<account_number>",
      "latest_balance": <Decimal>,
      "version": <int>,
      "updated": "<iso8601>",
      "updated_name": "<readable>"
    }
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

    state_key = {"pk": _state_pk(account_number)}

    # small bounded retry for race on initial state creation
    for _attempt in range(1, 6):
        try:
            state_resp = table.get_item(Key=state_key)
            state_item = state_resp.get("Item")
            state_exists = bool(state_item)

            if state_exists:
                latest_balance = _to_decimal(state_item.get("latest_balance")) or Decimal("0")
                version = state_item.get("version")
                try:
                    version = int(version)
                except Exception:
                    version = 0
            else:
                latest_balance = Decimal("0")
                version = 0

            balance_before = latest_balance
            if typ == "credit":
                balance_after = balance_before + amount
            else:
                balance_after = balance_before - amount

            # optional: block negative balances
            # if balance_after < 0:
            #     return {"is_created": False, "message": "insufficient balance", "ledger_id": ledger_id}

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
                "balance_before": balance_before,
                "amount": amount,
                "balance_after": balance_after,
            }

            if state_exists:
                # Transact:
                # 1) ConditionCheck state version unchanged
                # 2) Update state latest_balance + bump version
                # 3) Put ledger (unique pk)
                ddb_client.transact_write_items(
                    TransactItems=[
                        {
                            "ConditionCheck": {
                                "TableName": TABLE_NAME,
                                "Key": {"pk": _ddb(state_key["pk"])},
                                "ConditionExpression": "#v = :v",
                                "ExpressionAttributeNames": {"#v": "version"},
                                "ExpressionAttributeValues": {":v": _ddb(version)},
                            }
                        },
                        {
                            "Update": {
                                "TableName": TABLE_NAME,
                                "Key": {"pk": _ddb(state_key["pk"])},
                                "UpdateExpression": "SET #lb = :lb, #v = :v2, #u = :u, #un = :un",
                                "ExpressionAttributeNames": {
                                    "#lb": "latest_balance",
                                    "#v": "version",
                                    "#u": "updated",
                                    "#un": "updated_name",
                                },
                                "ExpressionAttributeValues": {
                                    ":lb": _ddb(balance_after),
                                    ":v2": _ddb(version + 1),
                                    ":u": _ddb(created_iso),
                                    ":un": _ddb(created_name),
                                },
                            }
                        },
                        {
                            "Put": {
                                "TableName": TABLE_NAME,
                                "Item": {k: _ddb(v) for k, v in ledger_item.items()},
                                "ConditionExpression": "attribute_not_exists(pk)",
                            }
                        },
                    ]
                )
            else:
                # Transact:
                # 1) Put initial state (must not exist)
                # 2) Put ledger (unique pk)
                state_item_new = {
                    "pk": state_key["pk"],
                    "kind": "account_state",
                    "account_number": account_number,
                    "latest_balance": balance_after,
                    "version": 1,
                    "updated": created_iso,
                    "updated_name": created_name,
                }

                ddb_client.transact_write_items(
                    TransactItems=[
                        {
                            "Put": {
                                "TableName": TABLE_NAME,
                                "Item": {k: _ddb(v) for k, v in state_item_new.items()},
                                "ConditionExpression": "attribute_not_exists(pk)",
                            }
                        },
                        {
                            "Put": {
                                "TableName": TABLE_NAME,
                                "Item": {k: _ddb(v) for k, v in ledger_item.items()},
                                "ConditionExpression": "attribute_not_exists(pk)",
                            }
                        },
                    ]
                )

            return {
                "is_created": True,
                "message": "Created",
                "ledger_id": ledger_id,
                "balance_before": float(balance_before),
                "balance_after": float(balance_after),
            }

        except ClientError as e:
            code = (e.response or {}).get("Error", {}).get("Code", "")
            if code in ("TransactionCanceledException", "ConditionalCheckFailedException"):
                # race: state version changed, or state was created by another writer -> retry
                continue
            msg = (e.response or {}).get("Error", {}).get("Message", "")
            return {"is_created": False, "message": f"DynamoDB error: {code}: {msg}".strip(), "ledger_id": ledger_id}
        except Exception as e:
            return {"is_created": False, "message": f"Unexpected error: {str(e)}", "ledger_id": ledger_id}

    return {"is_created": False, "message": "conflict: too many concurrent updates, retry", "ledger_id": ledger_id}


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

import os
import json
from decimal import Decimal

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

LEDGERS_TABLE = os.getenv("LEDGERS_TABLE") or "jef-wallets-ledgers"
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"

_ddb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
_table = _ddb.Table(LEDGERS_TABLE)


def _to_number(x, default=0):
    if x is None:
        return default
    if isinstance(x, Decimal):
        if x % 1 == 0:
            return int(x)
        return float(x)
    if isinstance(x, (int, float)):
        return x
    try:
        s = str(x).strip()
        if s == "":
            return default
        if "." in s:
            return float(s)
        return int(s)
    except Exception:
        return default


def _safe_str(x):
    return "" if x is None else str(x)


def _json_default(o):
    if isinstance(o, Decimal):
        if o % 1 == 0:
            return int(o)
        return float(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _normalize_ledger(it: dict) -> dict:
    # Response shape: account_* + ledger_id + balance fields (your schema)
    return {
        "account_number": _safe_str(it.get("account_number")),
        "sender_account_number": _safe_str(it.get("sender_account_number")),
        "sender_account_name": _safe_str(it.get("sender_account_name")),
        "receiver_account_number": _safe_str(it.get("receiver_account_number")),
        "receiver_account_name": _safe_str(it.get("receiver_account_name")),
        "ledger_id": _safe_str(it.get("ledger_id") or it.get("sk")),
        "date": _safe_str(it.get("date")),
        "date_name": _safe_str(it.get("date_name")),
        "created": _safe_str(it.get("created")),
        "created_name": _safe_str(it.get("created_name")),
        "created_by": _safe_str(it.get("created_by")),
        "type": _safe_str(it.get("type")),
        "description": _safe_str(it.get("description")),
        "balance_before": _to_number(it.get("balance_before"), 0),
        "amount": _to_number(it.get("amount"), 0),
        "balance_after": _to_number(it.get("balance_after"), 0),
    }


def get_all_ledgers_by_entity_number(entity_number: str) -> dict:
    # pk = entity_number
    entity_number = _safe_str(entity_number).strip()
    if not entity_number:
        return {"exists": False, "message": "Missing entity_number.", "ledgers": []}

    items = []
    try:
        last_evaluated_key = None

        while True:
            kwargs = {
                "KeyConditionExpression": Key("pk").eq(entity_number),
            }
            if last_evaluated_key:
                kwargs["ExclusiveStartKey"] = last_evaluated_key

            resp = _table.query(**kwargs)
            items.extend(resp.get("Items", []) or [])

            last_evaluated_key = resp.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        ledgers = [_normalize_ledger(it) for it in items]

        if not ledgers:
            return {
                "exists": False,
                "message": f"No ledger(s) found for entity_number {entity_number}.",
                "ledgers": [],
            }

        # Stable sort (since SK is uuid). If you later change SK to created/date, remove this and rely on query order.
        ledgers.sort(key=lambda x: (x.get("date", ""), x.get("created", ""), x.get("ledger_id", "")))

        return {"exists": True, "message": f"Found {len(ledgers)} ledger(s).", "ledgers": ledgers}

    except ClientError as e:
        return {
            "exists": False,
            "message": f"DynamoDB error: {e.response.get('Error', {}).get('Message', str(e))}",
            "ledgers": [],
        }
    except Exception as e:
        return {"exists": False, "message": f"Error: {str(e)}", "ledgers": []}


def _extract_entity_number(event: dict) -> str:
    # supports:
    # - event["entity_number"]
    # - JSON body {"entity_number":"1001"}
    # - queryStringParameters.entity_number
    if not isinstance(event, dict):
        return ""

    v = event.get("entity_number")
    if v is not None:
        return _safe_str(v).strip()

    body = event.get("body")
    if isinstance(body, str) and body.strip():
        try:
            j = json.loads(body)
            if isinstance(j, dict) and j.get("entity_number") is not None:
                return _safe_str(j.get("entity_number")).strip()
        except Exception:
            pass

    qsp = event.get("queryStringParameters") or {}
    if isinstance(qsp, dict) and qsp.get("entity_number") is not None:
        return _safe_str(qsp.get("entity_number")).strip()

    return ""


def lambda_handler(event, context):
    entity_number = _extract_entity_number(event)
    out = get_all_ledgers_by_entity_number(entity_number)

    status_code = 200 if out.get("exists") else 404
    if out.get("message") == "Missing entity_number.":
        status_code = 400

    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(out, default=_json_default, ensure_ascii=False),
    }

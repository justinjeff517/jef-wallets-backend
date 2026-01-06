import os
import json
from decimal import Decimal

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

LEDGERS_TABLE = os.getenv("LEDGERS_TABLE") or "jef-wallets-ledgers"
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"

_ddb = boto3.resource("dynamodb", region_name=AWS_REGION, config=Config(retries={"max_attempts": 10, "mode": "standard"}))
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

def _normalize_ledger(it: dict) -> dict:
    return {
        "entity_number": _safe_str(it.get("entity_number")),
        "transaction_number": _safe_str(it.get("transaction_number")),
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
    entity_number = _safe_str(entity_number).strip()
    if not entity_number:
        return {"exists": False, "message": "Missing entity_number.", "ledgers": []}

    items = []
    try:
        last_evaluated_key = None

        while True:
            kwargs = {
                "KeyConditionExpression": Key("pk").eq(entity_number),
                "ScanIndexForward": True,  # oldest -> newest; set False for newest -> oldest
            }
            if last_evaluated_key:
                kwargs["ExclusiveStartKey"] = last_evaluated_key

            resp = _table.query(**kwargs)
            items.extend(resp.get("Items", []))

            last_evaluated_key = resp.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        ledgers = [_normalize_ledger(it) for it in items]

        if not ledgers:
            return {"exists": False, "message": f"No ledger(s) found for entity_number {entity_number}.", "ledgers": []}

        return {"exists": True, "message": f"Found {len(ledgers)} ledger(s).", "ledgers": ledgers}

    except ClientError as e:
        return {"exists": False, "message": f"DynamoDB error: {e.response.get('Error', {}).get('Message', str(e))}", "ledgers": []}
    except Exception as e:
        return {"exists": False, "message": f"Error: {str(e)}", "ledgers": []}

# ---- Example local run ----
if __name__ == "__main__":
    payload = {"entity_number": "1003"}
    out = get_all_ledgers_by_entity_number(payload.get("entity_number"))
    print(json.dumps(out, ensure_ascii=False, indent=2))

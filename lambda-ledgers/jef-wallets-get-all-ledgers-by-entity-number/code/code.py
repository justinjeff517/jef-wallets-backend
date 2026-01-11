import os
import json
from decimal import Decimal
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# --- Config ---
LEDGERS_TABLE = os.getenv("LEDGERS_TABLE", "jef-wallets-ledgers")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(LEDGERS_TABLE)


# --- JSON helper (Decimal -> int/float) ---
def _json_default(o):
    if isinstance(o, Decimal):
        if o % 1 == 0:
            return int(o)
        return float(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _as_str(v: Any) -> str:
    return v.strip() if isinstance(v, str) else ""


def get_all_ledgers_by_entity_number(entity_number: str) -> Dict[str, Any]:
    entity_number = _as_str(entity_number)
    if not entity_number:
        return {
            "exists": False,
            "message": "entity_number is required",
            "ledgers": [],
        }

    ledgers: List[Dict[str, Any]] = []
    last_evaluated_key: Optional[Dict[str, Any]] = None

    try:
        while True:
            req: Dict[str, Any] = {
                "KeyConditionExpression": Key("pk").eq(entity_number),
            }
            if last_evaluated_key:
                req["ExclusiveStartKey"] = last_evaluated_key

            resp = table.query(**req)

            items = resp.get("Items", []) or []
            for it in items:
                # map to your response ledger shape; tolerate missing keys
                ledgers.append(
                    {
                        "account_number": it.get("account_number", ""),
                        "sender_account_number": it.get("sender_account_number", ""),
                        "sender_account_name": it.get("sender_account_name", ""),
                        "receiver_account_number": it.get("receiver_account_number", ""),
                        "receiver_account_name": it.get("receiver_account_name", ""),
                        "ledger_id": it.get("ledger_id", it.get("sk", "")),
                        "date": it.get("date", ""),
                        "date_name": it.get("date_name", ""),
                        "created": it.get("created", ""),
                        "created_name": it.get("created_name", ""),
                        "created_by": it.get("created_by", ""),
                        "type": it.get("type", ""),
                        "description": it.get("description", ""),
                        "balance_before": it.get("balance_before", 0),
                        "amount": it.get("amount", 0),
                        "balance_after": it.get("balance_after", 0),
                    }
                )

            last_evaluated_key = resp.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        if not ledgers:
            return {
                "exists": False,
                "message": f"No ledgers found for entity_number {entity_number}",
                "ledgers": [],
            }

        # Optional: if sk/ledger_id is time-sortable, keep; otherwise this is just a stable sort
        ledgers.sort(key=lambda x: (x.get("date", ""), x.get("created", ""), x.get("ledger_id", "")))

        return {
            "exists": True,
            "message": f"Found {len(ledgers)} ledgers for entity_number {entity_number}",
            "ledgers": ledgers,
        }

    except ClientError as e:
        return {
            "exists": False,
            "message": f"DynamoDB error: {e.response.get('Error', {}).get('Message', str(e))}",
            "ledgers": [],
        }
    except Exception as e:
        return {
            "exists": False,
            "message": f"Unexpected error: {str(e)}",
            "ledgers": [],
        }


# --- Example usage (Jupyter / script) ---
payload = {"entity_number": "1001"}
result = get_all_ledgers_by_entity_number(payload.get("entity_number"))

print(json.dumps(result, default=_json_default, ensure_ascii=False, indent=2))

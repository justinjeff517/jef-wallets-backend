import os
from decimal import Decimal
from typing import Dict, Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.exceptions import ClientError


LEDGERS_TABLE = os.getenv("LEDGERS_TABLE") or "jef-wallets-ledgers"
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"


def _to_decimal(x, default=None) -> Decimal:
    if x is None:
        return default
    if isinstance(x, Decimal):
        return x
    if isinstance(x, (int, float)):
        return Decimal(str(x))
    try:
        s = str(x).strip()
        if s == "":
            return default
        return Decimal(s)
    except Exception:
        return default


def verify_sufficient_funds(payload: Dict[str, Any]) -> Dict[str, Any]:
    entity_number = str((payload or {}).get("entity_number", "")).strip()
    amount = _to_decimal((payload or {}).get("amount"), default=None)

    if not entity_number:
        return {"is_sufficient": False, "message": "Missing entity_number.", "entity_number": ""}

    if amount is None:
        return {"is_sufficient": False, "message": "Missing or invalid amount.", "entity_number": entity_number}

    if amount < 0:
        return {"is_sufficient": False, "message": "Amount must be non-negative.", "entity_number": entity_number}

    ddb = boto3.resource(
        "dynamodb",
        region_name=AWS_REGION,
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )
    table = ddb.Table(LEDGERS_TABLE)

    try:
        resp = table.query(
            KeyConditionExpression=Key("pk").eq(entity_number),
            ScanIndexForward=False,
            Limit=1,
            ConsistentRead=False,
        )

        items = resp.get("Items") or []
        if not items:
            return {
                "is_sufficient": False,
                "message": "No ledger entries found for this entity_number.",
                "entity_number": entity_number,
            }

        it = items[0]
        bal = _to_decimal(it.get("balance_after"), default=None)
        if bal is None:
            bal = _to_decimal(it.get("balance_before"), default=Decimal("0"))

        if bal >= amount:
            return {"is_sufficient": True, "message": "Sufficient funds.", "entity_number": entity_number}

        return {"is_sufficient": False, "message": "Insufficient funds.", "entity_number": entity_number}

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return {"is_sufficient": False, "message": f"DynamoDB error: {msg}", "entity_number": entity_number}
    except Exception as e:
        return {"is_sufficient": False, "message": f"Unexpected error: {str(e)}", "entity_number": entity_number}


# example
print(verify_sufficient_funds({"entity_number": "1001", "amount": 250000}))

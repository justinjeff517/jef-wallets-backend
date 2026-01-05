import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Dict, Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.exceptions import ClientError


LEDGERS_TABLE = os.getenv("LEDGERS_TABLE") or "jef-wallets-ledgers"
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"
MANILA_TZ = timezone(timedelta(hours=8))


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
        d = Decimal(s)
        if d % 1 == 0:
            return int(d)
        return float(d)
    except Exception:
        return default


def _safe_date_name(yyyy_mm_dd: str) -> str:
    s = (yyyy_mm_dd or "").strip()
    if len(s) != 10:
        return ""
    try:
        dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=MANILA_TZ)
        return dt.strftime("%B %d, %Y, %A")
    except Exception:
        return ""


def get_latest_balance(payload: Dict[str, Any]) -> Dict[str, Any]:
    entity_number = str((payload or {}).get("entity_number", "")).strip()

    if not entity_number:
        return {
            "exists": False,
            "message": "Missing entity_number.",
            "entity_number": "",
            "latest_ledger_date": "",
            "latest_ledger_date_name": "",
            "latest_balance_amount": 0,
        }

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
                "exists": False,
                "message": "No ledger entries found for this entity_number.",
                "entity_number": entity_number,
                "latest_ledger_date": "",
                "latest_ledger_date_name": "",
                "latest_balance_amount": 0,
            }

        it = items[0]

        latest_date = str(it.get("date") or "").strip()
        if not latest_date:
            created = str(it.get("created") or "").strip()
            latest_date = created[:10] if len(created) >= 10 else ""

        latest_balance = _to_number(it.get("balance_after"), default=None)
        if latest_balance is None:
            latest_balance = _to_number(it.get("balance_before"), default=0)

        latest_date_name = _safe_date_name(latest_date)

        return {
            "exists": True,
            "message": "Latest balance found.",
            "entity_number": entity_number,
            "latest_ledger_date": latest_date,
            "latest_ledger_date_name": latest_date_name,
            "latest_balance_amount": latest_balance,
        }

    except ClientError as e:
        return {
            "exists": False,
            "message": f"DynamoDB error: {e.response.get('Error', {}).get('Message', str(e))}",
            "entity_number": entity_number,
            "latest_ledger_date": "",
            "latest_ledger_date_name": "",
            "latest_balance_amount": 0,
        }
    except Exception as e:
        return {
            "exists": False,
            "message": f"Unexpected error: {str(e)}",
            "entity_number": entity_number,
            "latest_ledger_date": "",
            "latest_ledger_date_name": "",
            "latest_balance_amount": 0,
        }


# example
print(get_latest_balance({"entity_number": "1003"}))

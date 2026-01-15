import os
import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config

AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
TABLE_NAME = (os.getenv("WALLETS_LEDGERS_TABLE") or "jef-wallets-ledgers").strip()
GSI_1_NAME = (os.getenv("WALLETS_LEDGERS_GSI_1_NAME") or "gsi_1").strip()

_TZ_MANILA = timezone(timedelta(hours=8))

ddb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
table = ddb.Table(TABLE_NAME)

def _as_str(v):
    return v.strip() if isinstance(v, str) else ""

def _to_number(v):
    if isinstance(v, Decimal):
        try:
            if v % 1 == 0:
                return int(v)
        except Exception:
            pass
        return float(v)
    return v

def _resp(status: int, body: dict):
    return {
        "statusCode": status,
        "headers": {
            "content-type": "application/json",
            "access-control-allow-origin": "*",
            "access-control-allow-headers": "*",
            "access-control-allow-methods": "OPTIONS,POST,GET",
        },
        "body": json.dumps(body, ensure_ascii=False, default=_to_number),
    }

def get_latest_balance(payload: dict):
    account_number = _as_str((payload or {}).get("account_number"))

    now = datetime.now(_TZ_MANILA)
    default_date_name = now.strftime("%B %d, %Y, %A")

    if not account_number:
        return {
            "exists": False,
            "message": "account_number is required",
            "reference_date_name": default_date_name,
            "latest_balance": 0,
        }

    resp = table.query(
        IndexName=GSI_1_NAME,
        KeyConditionExpression=Key("gsi_1_pk").eq(account_number),
        ScanIndexForward=False,
        Limit=1,
    )

    items = resp.get("Items") or []
    if not items:
        return {
            "exists": False,
            "message": "No ledger items found",
            "reference_date_name": default_date_name,
            "latest_balance": 0,
        }

    it = items[0]

    bal = _to_number(it.get("balance_after", 0)) or 0

    reference_date_name = _as_str(it.get("date_name"))
    reference_date_name = reference_date_name or default_date_name

    return {
        "exists": True,
        "message": "OK",
        "reference_date_name": reference_date_name,
        "latest_balance": bal,
    }

def _parse_event_payload(event):
    if not isinstance(event, dict):
        return {}
    if isinstance(event.get("body"), str) and event["body"].strip():
        try:
            return json.loads(event["body"])
        except Exception:
            return {}
    if isinstance(event.get("queryStringParameters"), dict):
        return event["queryStringParameters"] or {}
    return event

def lambda_handler(event, context):
    try:
        payload = _parse_event_payload(event)
        result = get_latest_balance(payload)
        return _resp(200, result)
    except Exception as e:
        now = datetime.now(_TZ_MANILA)
        return _resp(
            500,
            {
                "exists": False,
                "message": "Internal server error",
                "reference_date_name": now.strftime("%B %d, %Y, %A"),
                "latest_balance": 0,
                "error": str(e),
            },
        )

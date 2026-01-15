import os
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

def get_latest_balance(payload: dict):
    account_number = (payload or {}).get("account_number")
    account_number = account_number.strip() if isinstance(account_number, str) else ""

    now = datetime.now(_TZ_MANILA)
    default_date = now.strftime("%Y-%m-%d")
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

    bal = it.get("balance_after", 0)
    if isinstance(bal, Decimal):
        bal = float(bal)

    reference_date_name = it.get("date_name") if isinstance(it.get("date_name"), str) else ""
    reference_date_name = reference_date_name.strip() or default_date_name

    return {
        "exists": True,
        "message": "OK",
        "reference_date_name": reference_date_name,
        "latest_balance": bal,
    }

if __name__ == "__main__":
    import json
    payload = {"account_number": "1001"}  # change this
    res = get_latest_balance(payload)
    print(json.dumps(res, indent=2))

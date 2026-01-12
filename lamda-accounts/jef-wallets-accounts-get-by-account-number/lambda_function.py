import os
import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

TABLE_NAME = os.getenv("ACCOUNTS_TABLE") or "jef-wallets-accounts"
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"

_ddb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
_table = _ddb.Table(TABLE_NAME)


def _json_body(event):
    if isinstance(event, dict):
        b = event.get("body")
        if isinstance(b, str) and b.strip():
            try:
                return json.loads(b)
            except Exception:
                return {}
        if isinstance(b, dict):
            return b
        if "account_number" in event:
            return event
    return {}


def _resp(status_code, body):
    return {
        "statusCode": int(status_code),
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body),
    }


def lambda_handler(event, context):
    payload = _json_body(event)
    account_number = str(payload.get("account_number") or "").strip()

    if not account_number:
        return _resp(
            400,
            {
                "exists": False,
                "message": "account_number is required.",
                "account": None,
            },
        )

    try:
        resp = _table.get_item(Key={"pk": account_number})
        item = resp.get("Item")

        if not item:
            return _resp(
                404,
                {
                    "exists": False,
                    "message": f"Account not found for account_number={account_number}.",
                    "account": None,
                },
            )

        return _resp(
            200,
            {
                "exists": True,
                "message": "Account found.",
                "account": {
                    "account_number": item.get("account_number") or item.get("pk") or account_number,
                    "account_name": item.get("account_name") or "",
                },
            },
        )

    except ClientError as e:
        return _resp(
            500,
            {
                "exists": False,
                "message": f"DynamoDB ClientError: {e.response.get('Error', {}).get('Message', str(e))}",
                "account": None,
            },
        )
    except Exception as e:
        return _resp(
            500,
            {
                "exists": False,
                "message": f"Unhandled error: {str(e)}",
                "account": None,
            },
        )

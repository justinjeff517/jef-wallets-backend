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


def get_account_by_account_number(payload: dict) -> dict:
    account_number = str(payload.get("account_number") or "").strip()
    if not account_number:
        return {
            "exists": False,
            "message": "account_number is required.",
            "account": None,
        }

    try:
        resp = _table.get_item(Key={"pk": account_number})
        item = resp.get("Item")

        if not item:
            return {
                "exists": False,
                "message": f"Account not found for account_number={account_number}.",
                "account": None,
            }

        return {
            "exists": True,
            "message": "Account found.",
            "account": {
                "account_number": item.get("account_number") or item.get("pk") or account_number,
                "account_name": item.get("account_name") or "",
            },
        }

    except ClientError as e:
        return {
            "exists": False,
            "message": f"DynamoDB ClientError: {e.response.get('Error', {}).get('Message', str(e))}",
            "account": None,
        }
    except Exception as e:
        return {
            "exists": False,
            "message": f"Unhandled error: {str(e)}",
            "account": None,
        }


if __name__ == "__main__":
    payload = {"account_number": "1001"}
    print(json.dumps(get_account_by_account_number(payload), indent=2))

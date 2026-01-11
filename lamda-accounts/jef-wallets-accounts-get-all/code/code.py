import os
import json
from decimal import Decimal

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer

ACCOUNTS_TABLE = os.getenv("ACCOUNTS_TABLE") or "jef-wallets-accounts"
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"

_ddb = boto3.client(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)

_deser = TypeDeserializer()


def _ddb_item_to_py(item: dict) -> dict:
    return {k: _deser.deserialize(v) for k, v in (item or {}).items()}


def _json_default(o):
    if isinstance(o, Decimal):
        if o % 1 == 0:
            return int(o)
        return float(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def get_all_accounts(table_name: str = ACCOUNTS_TABLE) -> dict:
    try:
        accounts = []
        start_key = None

        while True:
            req = {"TableName": table_name}
            if start_key:
                req["ExclusiveStartKey"] = start_key

            resp = _ddb.scan(**req)

            for raw in resp.get("Items", []) or []:
                it = _ddb_item_to_py(raw)
                account_number = str(it.get("account_number", "")).strip()
                account_name = str(it.get("account_name", "")).strip()
                if account_number:
                    accounts.append(
                        {
                            "account_number": account_number,
                            "account_name": account_name,
                        }
                    )

            start_key = resp.get("LastEvaluatedKey")
            if not start_key:
                break

        accounts.sort(key=lambda x: x.get("account_number", ""))

        if not accounts:
            return {"exists": False, "message": "No accounts found.", "accounts": []}

        return {"exists": True, "message": "Accounts found.", "accounts": accounts}

    except ClientError as e:
        msg = (e.response.get("Error") or {}).get("Message") or str(e)
        return {"exists": False, "message": f"DynamoDB error: {msg}", "accounts": []}
    except Exception as e:
        return {"exists": False, "message": f"Error: {str(e)}", "accounts": []}


if __name__ == "__main__":
    out = get_all_accounts()
    print(json.dumps(out, default=_json_default, indent=2))

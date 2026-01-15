import os
import json

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr

TABLE_NAME = os.getenv("LEDGERS_TABLE") or "jef-wallets-ledgers"
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"

_dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
_table = _dynamodb.Table(TABLE_NAME)

# Your table uses only "pk" (ledger_id) — no sort key
PK_NAME = "pk"
SK_NAME = None  # important: set to None

# Safety guard: only delete items where account_number matches this value
# Leave empty ("") to allow deleting everything (dangerous!)
DELETE_ACCOUNT_NUMBER = os.getenv("DELETE_ACCOUNT_NUMBER", "").strip()


def delete_all_ledgers():
    deleted_count = 0
    try:
        last_evaluated_key = None

        scan_kwargs = {
            "ProjectionExpression": PK_NAME  # only need pk for delete
        }

        # Optional safety filter
        if DELETE_ACCOUNT_NUMBER:
            scan_kwargs["FilterExpression"] = Attr("account_number").eq(DELETE_ACCOUNT_NUMBER)

        while True:
            if last_evaluated_key:
                scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

            response = _table.scan(**scan_kwargs)
            items = response.get("Items", [])

            if items:
                with _table.batch_writer() as batch:
                    for item in items:
                        pk_value = item.get(PK_NAME)
                        if pk_value is None:
                            continue

                        key = {PK_NAME: pk_value}
                        batch.delete_item(Key=key)
                        deleted_count += 1

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        if deleted_count == 0:
            msg = "No ledgers found."
            if DELETE_ACCOUNT_NUMBER:
                msg += f" (filtered by account_number={DELETE_ACCOUNT_NUMBER})"
            return {
                "is_deleted": False,
                "deleted_count": 0,
                "message": msg,
            }

        scope = f" for account_number={DELETE_ACCOUNT_NUMBER}" if DELETE_ACCOUNT_NUMBER else " (all items)"
        return {
            "is_deleted": True,
            "deleted_count": deleted_count,
            "message": f"Deleted {deleted_count} ledger(s){scope}.",
        }

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return {
            "is_deleted": False,
            "deleted_count": deleted_count,
            "message": f"DynamoDB error: {msg}",
        }
    except Exception as e:
        return {
            "is_deleted": False,
            "deleted_count": deleted_count,
            "message": f"Unexpected error: {str(e)}",
        }


def lambda_handler(event, context=None):
    result = delete_all_ledgers()
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(result, ensure_ascii=False, default=str),
    }


if __name__ == "__main__":
    print(json.dumps(delete_all_ledgers(), indent=2, ensure_ascii=False, default=str))
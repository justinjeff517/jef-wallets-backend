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

# If your table key schema is NOT (pk, sk), set these to match your real keys.
# Example if keys are (account_number, ledger_id):
#   PK_NAME = "account_number"
#   SK_NAME = "ledger_id"
PK_NAME = os.getenv("PK_NAME") or "pk"
SK_NAME = os.getenv("SK_NAME") or "sk"

# Optional safety guard:
# - If set, only delete items where attribute "account_number" equals this value.
#   (prevents wiping the whole table by mistake)
DELETE_ACCOUNT_NUMBER = os.getenv("DELETE_ACCOUNT_NUMBER", "").strip()


def delete_all_ledgers():
    deleted_count = 0
    try:
        last_evaluated_key = None

        # Build scan args
        scan_kwargs = {"ProjectionExpression": f"{PK_NAME}, {SK_NAME}"}

        # Optional filter (safe mode)
        if DELETE_ACCOUNT_NUMBER:
            # NOTE: FilterExpression attribute name is fixed here ("account_number").
            # If you want it dynamic, make it an env too.
            scan_kwargs["FilterExpression"] = Attr("account_number").eq(DELETE_ACCOUNT_NUMBER)

        while True:
            if last_evaluated_key:
                scan_kwargs["ExclusiveStartKey"] = last_evaluated_key
            else:
                scan_kwargs.pop("ExclusiveStartKey", None)

            resp = _table.scan(**scan_kwargs)
            items = resp.get("Items", []) or []

            if items:
                with _table.batch_writer() as batch:
                    for it in items:
                        pk = it.get(PK_NAME)
                        sk = it.get(SK_NAME)

                        # If your table is single-key, SK_NAME might not exist; handle that.
                        if pk is None:
                            continue

                        key = {PK_NAME: pk}
                        if sk is not None and SK_NAME:
                            key[SK_NAME] = sk

                        batch.delete_item(Key=key)
                        deleted_count += 1

            last_evaluated_key = resp.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        if deleted_count == 0:
            return {
                "is_deleted": False,
                "deleted_count": 0,
                "message": "No ledgers found to delete.",
            }

        scope = f" for account_number={DELETE_ACCOUNT_NUMBER}" if DELETE_ACCOUNT_NUMBER else ""
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
            "message": f"Error: {str(e)}",
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

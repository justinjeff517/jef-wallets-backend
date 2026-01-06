import os
import json

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

TABLE_NAME = os.getenv("LEDGERS_TABLE") or "jef-wallets-ledgers"
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"

_dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
_table = _dynamodb.Table(TABLE_NAME)


def delete_all_ledgers():
    deleted_count = 0
    try:
        last_evaluated_key = None

        while True:
            kwargs = {"ProjectionExpression": "pk, sk"}
            if last_evaluated_key:
                kwargs["ExclusiveStartKey"] = last_evaluated_key

            resp = _table.scan(**kwargs)
            items = resp.get("Items", []) or []

            if items:
                with _table.batch_writer() as batch:
                    for it in items:
                        pk = it.get("pk")
                        sk = it.get("sk")
                        if pk is not None and sk is not None:
                            batch.delete_item(Key={"pk": pk, "sk": sk})
                            deleted_count += 1

            last_evaluated_key = resp.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        if deleted_count == 0:
            return {"is_deleted": False, "deleted_count": 0, "message": "No ledgers found to delete."}

        return {"is_deleted": True, "deleted_count": deleted_count, "message": f"Deleted {deleted_count} ledger(s)."}

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return {"is_deleted": False, "deleted_count": deleted_count, "message": f"DynamoDB error: {msg}"}
    except Exception as e:
        return {"is_deleted": False, "deleted_count": deleted_count, "message": f"Error: {str(e)}"}


def lambda_handler(event, context=None):
    # No params required
    result = delete_all_ledgers()
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(result),
    }


if __name__ == "__main__":
    print(delete_all_ledgers())

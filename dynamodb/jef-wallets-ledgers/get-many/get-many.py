import json
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError
from botocore.config import Config

TABLE_NAME = "jef-wallets-ledgers"

# Use resource (recommended for table operations)
dynamodb = boto3.resource(
    "dynamodb",
    config=Config(retries={"max_attempts": 10, "mode": "standard"})
)
table = dynamodb.Table(TABLE_NAME)


def decimal_to_json_friendly(obj):
    """
    Convert Decimal to int (if whole number) or float.
    Other types → str as fallback.
    """
    if isinstance(obj, Decimal):
        if obj == obj.to_integral_value():
            return int(obj)
        return float(obj)
    return str(obj)


def get_all_items(page_limit=1000):
    items = []
    last_evaluated_key = None

    try:
        while True:
            scan_kwargs = {
                "Limit": page_limit,
            }
            if last_evaluated_key:
                scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

            response = table.scan(**scan_kwargs)

            # Add current page
            items.extend(response.get("Items", []))

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        return items

    except ClientError as e:
        print("DynamoDB error:", e.response["Error"]["Message"])
        return []
    except Exception as e:
        print("Unexpected error:", str(e))
        return []


# ────────────────────────────────────────────────
# Run
# ────────────────────────────────────────────────

print("Fetching all items... (this may take time if table is large)")

all_items = get_all_items()

print(f"Total count: {len(all_items)}")

if all_items:
    # Print nicely formatted JSON
    print(json.dumps(
        all_items,
        indent=2,
        ensure_ascii=False,
        default=decimal_to_json_friendly
    ))
else:
    print("No items found or error occurred.")
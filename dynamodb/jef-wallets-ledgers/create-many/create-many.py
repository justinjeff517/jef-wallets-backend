import json
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError

TABLE_NAME = "jef-wallets-ledgers"
FILE_PATH = r"H:\github12\jef-wallets-backend\dynamodb\jef-wallets-ledgers\create-many\datas.json"

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

def load_items(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f, parse_float=Decimal, parse_int=Decimal)

    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise ValueError("datas.json must be an object or array of objects")

def validate_item(item):
    if not isinstance(item, dict):
        raise ValueError("Each item must be a dictionary/object")

    # Required: pk must exist and be non-empty string (ledger_id)
    pk = item.get("pk")
    if not isinstance(pk, str) or not pk.strip():
        raise ValueError("Missing or invalid 'pk' (must be non-empty string)")

    # Strongly recommended: ledger_id should match pk
    ledger_id = item.get("ledger_id")
    if ledger_id is None:
        item["ledger_id"] = pk
    elif ledger_id != pk:
        raise ValueError(f"'ledger_id' ({ledger_id}) does not match 'pk' ({pk})")

    # Convert number fields to Decimal if they come as int/float/str
    for key in ("balance_before", "amount", "balance_after"):
        if key in item and item[key] is not None:
            val = item[key]
            if isinstance(val, (int, float)):
                item[key] = Decimal(str(val))  # avoid float precision issues
            elif isinstance(val, str):
                try:
                    item[key] = Decimal(val)
                except:
                    raise ValueError(f"Cannot convert {key} to Decimal: {val}")
            elif not isinstance(val, Decimal):
                raise ValueError(f"Invalid type for {key}: {type(val).__name__}")

    # Optional: you can add more required field checks here
    required_strings = [
        "account_number", "type", "description", "date", "created"
    ]
    for field in required_strings:
        if field not in item or not isinstance(item[field], str) or not item[field].strip():
            raise ValueError(f"Missing or empty required field: {field}")

    if item["type"] not in ("credit", "debit"):
        raise ValueError("type must be 'credit' or 'debit'")

    return item

def upload_items(items):
    ok = 0
    fail = 0
    errors = []

    # IMPORTANT: only "pk" is the primary key → no sort key
    overwrite_keys = ["pk"]

    try:
        with table.batch_writer(overwrite_by_pkeys=overwrite_keys) as batch:
            for i, raw_item in enumerate(items):
                try:
                    validated = validate_item(raw_item)
                    batch.put_item(Item=validated)
                    ok += 1
                except Exception as e:
                    fail += 1
                    errors.append({"index": i, "error": str(e)})
    except ClientError as e:
        # If the whole batch fails catastrophically
        return {
            "uploaded": ok,
            "failed": len(items) - ok,
            "errors": [{"batch_error": str(e)}]
        }

    return {
        "uploaded": ok,
        "failed": fail,
        "errors": errors[:20]   # limit error list size in output
    }

# ────────────────────────────────────────────────
# Run
# ────────────────────────────────────────────────

try:
    items = load_items(FILE_PATH)
    print(f"Loaded {len(items)} items from file")
except Exception as e:
    print(json.dumps({"error": f"Failed to load file: {str(e)}"}, indent=2))
    exit(1)

result = upload_items(items)
print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
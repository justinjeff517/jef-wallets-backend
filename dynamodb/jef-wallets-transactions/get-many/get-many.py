import os
import json
from decimal import Decimal
import boto3
from botocore.config import Config

# -----------------------------
# CONFIG
# -----------------------------
AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
TABLE_NAME = (os.getenv("WALLETS_TRANSACTIONS_TABLE") or "jef-wallets-transactions").strip()

# Optional: use a named AWS profile on your machine
AWS_PROFILE = (os.getenv("AWS_PROFILE") or "").strip()

# Optional: safety cap (prevents accidental full-table scan of huge tables)
MAX_ITEMS = int((os.getenv("MAX_ITEMS") or "10000").strip())

# -----------------------------
# JSON helpers (Decimal -> float)
# -----------------------------
def _json_default(o):
    if isinstance(o, Decimal):
        return float(o)
    return str(o)

# -----------------------------
# DDB
# -----------------------------
session = boto3.Session(profile_name=AWS_PROFILE) if AWS_PROFILE else boto3.Session()
ddb = session.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
table = ddb.Table(TABLE_NAME)

# -----------------------------
# GET ALL (Scan)
# -----------------------------
items = []
last_evaluated_key = None

while True:
    kwargs = {"Limit": min(1000, max(1, MAX_ITEMS - len(items)))}
    if last_evaluated_key:
        kwargs["ExclusiveStartKey"] = last_evaluated_key

    resp = table.scan(**kwargs)

    items.extend(resp.get("Items", []))
    last_evaluated_key = resp.get("LastEvaluatedKey")

    if len(items) >= MAX_ITEMS:
        break
    if not last_evaluated_key:
        break

out = {
    "count": len(items),
    "items": items,
    "truncated": len(items) >= MAX_ITEMS,
    "max_items": MAX_ITEMS,
}

print(json.dumps(out, ensure_ascii=False, indent=2, default=_json_default))

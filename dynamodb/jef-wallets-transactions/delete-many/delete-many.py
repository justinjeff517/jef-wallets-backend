import os
import json
import boto3
from botocore.config import Config
from decimal import Decimal

# -----------------------------
# CONFIG
# -----------------------------
AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
TABLE_NAME = (os.getenv("WALLETS_TRANSACTIONS_TABLE") or "jef-wallets-transactions").strip()

# Optional: use a named AWS profile on your machine
AWS_PROFILE = (os.getenv("AWS_PROFILE") or "").strip()

# Optional: safety caps (prevents accidental wipe of huge tables)
MAX_DELETE = int((os.getenv("MAX_DELETE") or "5000").strip())   # max items to delete in this run
SCAN_PAGE = int((os.getenv("SCAN_PAGE") or "500").strip())      # scan page size

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
# DELETE ALL (Scan + Batch delete)
# Assumes primary key is "pk" only (as per your schema).
# -----------------------------
deleted = 0
last_evaluated_key = None

with table.batch_writer() as batch:
    while True:
        if deleted >= MAX_DELETE:
            break

        limit = min(SCAN_PAGE, MAX_DELETE - deleted)
        scan_kwargs = {"Limit": limit}
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        resp = table.scan(**scan_kwargs)
        items = resp.get("Items", [])

        for it in items:
            pk = it.get("pk")
            if not pk:
                # skip anything malformed
                continue
            batch.delete_item(Key={"pk": pk})
            deleted += 1
            if deleted >= MAX_DELETE:
                break

        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key or not items:
            break

print(json.dumps({
    "table": TABLE_NAME,
    "region": AWS_REGION,
    "deleted": deleted,
    "truncated": deleted >= MAX_DELETE,
    "max_delete": MAX_DELETE
}, indent=2))

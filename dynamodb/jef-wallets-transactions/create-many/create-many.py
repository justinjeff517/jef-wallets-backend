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

JSON_PATH = r"H:\github12\jef-wallets-backend\dynamodb\jef-wallets-transactions\create-many\datas.json"

# Optional: if you want to use a named AWS profile on your machine
AWS_PROFILE = (os.getenv("AWS_PROFILE") or "").strip()


# -----------------------------
# HELPERS
# -----------------------------
def _as_str(v):
    return v.strip() if isinstance(v, str) else ""

def _to_decimal(obj):
    # DynamoDB expects Decimal for numbers (boto3 will reject float by default in some cases)
    return json.loads(json.dumps(obj), parse_float=Decimal, parse_int=Decimal)

def _ensure_keys(item: dict) -> dict:
    # Fill derived/index fields if missing
    txid = _as_str(item.get("transaction_id"))
    typ = _as_str(item.get("type"))
    created = _as_str(item.get("created"))

    if not _as_str(item.get("pk")) and txid and typ:
        item["pk"] = f"{txid}#{typ}"

    if not _as_str(item.get("gsi_1_pk")):
        item["gsi_1_pk"] = _as_str(item.get("sender_account_number"))
    if not _as_str(item.get("gsi_1_sk")):
        item["gsi_1_sk"] = created

    if not _as_str(item.get("gsi_2_pk")):
        item["gsi_2_pk"] = _as_str(item.get("receiver_account_number"))
    if not _as_str(item.get("gsi_2_sk")):
        item["gsi_2_sk"] = created

    return item


# -----------------------------
# LOAD JSON
# -----------------------------
with open(JSON_PATH, "r", encoding="utf-8") as f:
    raw = json.load(f)

items = raw.get("items") if isinstance(raw, dict) and "items" in raw else raw
if not isinstance(items, list):
    raise ValueError("datas.json must be a JSON array, or an object with an 'items' array.")

items = _to_decimal(items)

# -----------------------------
# DDB CLIENT
# -----------------------------
session = boto3.Session(profile_name=AWS_PROFILE) if AWS_PROFILE else boto3.Session()
ddb = session.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
table = ddb.Table(TABLE_NAME)

# -----------------------------
# UPLOAD (batch writer)
# -----------------------------
count = 0
with table.batch_writer(overwrite_by_pkeys=["pk"]) as batch:
    for it in items:
        if not isinstance(it, dict):
            raise ValueError("Each item must be a JSON object.")
        it = _ensure_keys(it)
        if not _as_str(it.get("pk")):
            raise ValueError(f"Missing pk after derivation. Item: {it}")
        batch.put_item(Item=it)
        count += 1

print(f"Uploaded {count} items to {TABLE_NAME} in {AWS_REGION}")

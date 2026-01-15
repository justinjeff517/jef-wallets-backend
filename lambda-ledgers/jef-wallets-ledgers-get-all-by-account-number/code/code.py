import os
import json
import time
import math
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key

# ----------------------------
# CONFIG
# ----------------------------
AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
TABLE_NAME = (os.getenv("WALLETS_LEDGERS_TABLE") or "jef-wallets-ledgers").strip()

# no zoneinfo; fixed offset
_TZ_MANILA = timezone(timedelta(hours=8))

# Read capacity limiter (RCU per second)
RCU_PER_SEC = 22
SAFETY = 0.9  # use 90% of limit

ddb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
table = ddb.Table(TABLE_NAME)

GSI_1_NAME = "gsi_1"
GSI_2_NAME = "gsi_2"

# ----------------------------
# HELPERS
# ----------------------------
def _as_str(v):
    return v.strip() if isinstance(v, str) else ""

def _dec_to_num(v):
    if isinstance(v, Decimal):
        if v % 1 == 0:
            return int(v)
        return float(v)
    return v

class RcuLimiter:
    """
    Simple 1-second window limiter using DynamoDB ConsumedCapacity units.
    """
    def __init__(self, rcu_per_sec=22, safety=0.9):
        self.limit = float(rcu_per_sec) * float(safety)
        self.window_start = time.monotonic()
        self.used = 0.0

    def consume(self, units):
        try:
            u = float(units or 0.0)
        except Exception:
            u = 0.0

        now = time.monotonic()
        elapsed = now - self.window_start

        # roll window if >= 1 second
        if elapsed >= 1.0:
            self.window_start = now
            self.used = 0.0

        self.used += u

        if self.used > self.limit:
            # sleep until next window
            now2 = time.monotonic()
            sleep_for = max(0.0, 1.0 - (now2 - self.window_start))
            if sleep_for > 0:
                time.sleep(sleep_for)
            # reset window after sleeping
            self.window_start = time.monotonic()
            self.used = 0.0

def _query_all(index_name, pk_name, pk_value, limiter: RcuLimiter | None = None):
    """
    Queries all items where <pk_name> == pk_value on the given GSI.
    Handles pagination and rate-limits by consumed RCU (CapacityUnits).
    """
    items = []
    last_evaluated_key = None

    while True:
        kwargs = {
            "IndexName": index_name,
            "KeyConditionExpression": Key(pk_name).eq(pk_value),
            "ReturnConsumedCapacity": "TOTAL",
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key

        resp = table.query(**kwargs)

        if limiter:
            cc = resp.get("ConsumedCapacity") or {}
            limiter.consume(cc.get("CapacityUnits"))

        items.extend(resp.get("Items", []))
        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    return items

def _normalize_item(it):
    ledger_id = _as_str(it.get("ledger_id")) or _as_str(it.get("pk"))
    return {
        "account_number": _as_str(it.get("account_number")),
        "sender_account_number": _as_str(it.get("sender_account_number")),
        "sender_account_name": _as_str(it.get("sender_account_name")),
        "receiver_account_number": _as_str(it.get("receiver_account_number")),
        "receiver_account_name": _as_str(it.get("receiver_account_name")),
        "ledger_id": ledger_id,
        "date": _as_str(it.get("date")),
        "date_name": _as_str(it.get("date_name")),
        "created": _as_str(it.get("created")),
        "created_name": _as_str(it.get("created_name")),
        "created_by": _as_str(it.get("created_by")),
        "type": _as_str(it.get("type")),
        "description": _as_str(it.get("description")),
        "amount": _dec_to_num(it.get("amount")),
        "elapsed_time": "",
    }

def list_ledgers_by_account(payload):
    """
    Payload:
      { "account_number": "string" }

    account_number is used for:
      - GSI_1: sender_account_number (gsi_1_pk)
      - GSI_2: receiver_account_number (gsi_2_pk)
    """
    t0 = time.perf_counter()

    account_number = _as_str((payload or {}).get("account_number"))
    if not account_number:
        return {"exists": False, "message": "Missing account_number", "ledgers": []}

    limiter = RcuLimiter(rcu_per_sec=RCU_PER_SEC, safety=SAFETY)

    try:
        out_items = _query_all(GSI_1_NAME, "gsi_1_pk", account_number, limiter=limiter)
        in_items = _query_all(GSI_2_NAME, "gsi_2_pk", account_number, limiter=limiter)

        merged = []
        seen = set()

        for it in out_items + in_items:
            lid = _as_str(it.get("ledger_id")) or _as_str(it.get("pk"))
            if not lid or lid in seen:
                continue
            seen.add(lid)
            merged.append(_normalize_item(it))

        merged.sort(key=lambda x: (x.get("created") or "", x.get("ledger_id") or ""), reverse=True)

        elapsed = time.perf_counter() - t0
        elapsed_str = f"{elapsed:.3f}s"
        for x in merged:
            x["elapsed_time"] = elapsed_str

        if not merged:
            return {"exists": False, "message": "No ledgers found", "ledgers": []}

        return {"exists": True, "message": "OK", "ledgers": merged}

    except Exception as e:
        return {"exists": False, "message": f"Error: {type(e).__name__}: {e}", "ledgers": []}

# Example:
payload = {"account_number": "1006"}
print(json.dumps(list_ledgers_by_account(payload), indent=2, default=str))

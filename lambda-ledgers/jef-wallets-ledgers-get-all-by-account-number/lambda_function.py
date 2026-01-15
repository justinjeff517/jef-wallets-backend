import os
import json
import time
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

_TZ_MANILA = timezone(timedelta(hours=8))

RCU_PER_SEC = 22
SAFETY = 0.9

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

def _json_loads_safe(s):
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        return json.loads(s)
    except Exception:
        return None

def _coerce_payload(event):
    """
    Accepts common Lambda event shapes:
    - API Gateway v1/v2: {"body": "...json..."} or {"body": {...}}
    - Direct invoke: {"account_number": "..."}
    - SQS: {"Records":[{"body":"...json..."}]}
    Returns: dict payload (first record for SQS)
    """
    if isinstance(event, dict) and isinstance(event.get("Records"), list) and event["Records"]:
        r0 = event["Records"][0] or {}
        body = r0.get("body")
        if isinstance(body, dict):
            return body
        p = _json_loads_safe(body)
        return p if isinstance(p, dict) else {}

    if isinstance(event, dict) and "body" in event:
        body = event.get("body")
        if isinstance(body, dict):
            return body
        p = _json_loads_safe(body)
        return p if isinstance(p, dict) else {}

    return event if isinstance(event, dict) else {}

class RcuLimiter:
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

        if elapsed >= 1.0:
            self.window_start = now
            self.used = 0.0

        self.used += u

        if self.used > self.limit:
            now2 = time.monotonic()
            sleep_for = max(0.0, 1.0 - (now2 - self.window_start))
            if sleep_for > 0:
                time.sleep(sleep_for)
            self.window_start = time.monotonic()
            self.used = 0.0

def _query_all(index_name, pk_name, pk_value, limiter: RcuLimiter | None = None):
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
    t0 = time.perf_counter()

    account_number = _as_str((payload or {}).get("account_number"))
    if not account_number:
        return {"exists": False, "message": "Missing account_number", "ledgers": []}

    limiter = RcuLimiter(rcu_per_sec=RCU_PER_SEC, safety=SAFETY)

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

def _http_response(status_code, obj):
    return {
        "statusCode": int(status_code),
        "headers": {
            "content-type": "application/json",
            "cache-control": "no-store",
        },
        "body": json.dumps(obj, ensure_ascii=False, default=str),
    }

# ----------------------------
# LAMBDA HANDLER
# ----------------------------
def lambda_handler(event, context):
    try:
        payload = _coerce_payload(event)
        result = list_ledgers_by_account(payload)
        # Keep your response format; return 200 even when exists=false (your choice)
        return _http_response(200, result)
    except Exception as e:
        return _http_response(
            500,
            {"exists": False, "message": f"Error: {type(e).__name__}: {e}", "ledgers": []},
        )

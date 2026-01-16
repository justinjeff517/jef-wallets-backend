import os
import json
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key


AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
TABLE_NAME = (os.getenv("WALLETS_TRANSACTIONS_TABLE") or "jef-wallets-transactions").strip()

GSI_1_NAME = (os.getenv("WALLETS_TRANSACTIONS_GSI1_NAME") or "gsi_1").strip()  # sender
GSI_2_NAME = (os.getenv("WALLETS_TRANSACTIONS_GSI2_NAME") or "gsi_2").strip()  # receiver

# -----------------------------
# RCU FREE-TIER / THROTTLE
# -----------------------------
RCU_LIMIT_PER_SECOND = float(os.getenv("RCU_LIMIT_PER_SECOND", "25") or 25)
RCU_HEADROOM = float(os.getenv("RCU_HEADROOM", "0.9") or 0.9)
QUERY_PAGE_LIMIT = int(os.getenv("QUERY_PAGE_LIMIT", "200") or 200)

_RCU_BUDGET = max(0.0, RCU_LIMIT_PER_SECOND * RCU_HEADROOM)


class _PerSecondRcuLimiter:
    def __init__(self, budget_per_sec: float):
        self.budget = float(budget_per_sec)
        self._win_start = time.monotonic()
        self._used = 0.0

    def _reset_if_needed(self):
        now = time.monotonic()
        if now - self._win_start >= 1.0:
            self._win_start = now
            self._used = 0.0

    def add_and_throttle(self, consumed_rcu: float):
        if self.budget <= 0:
            return

        self._reset_if_needed()
        self._used += float(consumed_rcu or 0.0)

        if self._used >= self.budget:
            now = time.monotonic()
            sleep_s = max(0.0, 1.0 - (now - self._win_start))
            if sleep_s > 0:
                time.sleep(sleep_s)
            self._win_start = time.monotonic()
            self._used = 0.0


_rcu_limiter = _PerSecondRcuLimiter(_RCU_BUDGET)

ddb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
table = ddb.Table(TABLE_NAME)


def _as_str(v: Any) -> str:
    return v.strip() if isinstance(v, str) else ""


def _json_default(o: Any):
    if isinstance(o, Decimal):
        if o % 1 == 0:
            return int(o)
        return float(o)
    return str(o)


def _query_all(IndexName: str, pk_name: str, pk_value: str, scan_forward: bool = False) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    eks: Optional[Dict[str, Any]] = None

    while True:
        kwargs: Dict[str, Any] = {
            "IndexName": IndexName,
            "KeyConditionExpression": Key(pk_name).eq(pk_value),
            "ScanIndexForward": scan_forward,
            "ReturnConsumedCapacity": "TOTAL",
            "Limit": QUERY_PAGE_LIMIT,
        }
        if eks:
            kwargs["ExclusiveStartKey"] = eks

        resp = table.query(**kwargs)

        cc = resp.get("ConsumedCapacity") or {}
        _rcu_limiter.add_and_throttle(cc.get("CapacityUnits", 0.0))

        items.extend(resp.get("Items") or [])
        eks = resp.get("LastEvaluatedKey")
        if not eks:
            break

    return items


def _map_sender(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "transaction_id": _as_str(item.get("transaction_id")),
        "counterparty_account_number": _as_str(item.get("receiver_account_number")),
        "counterparty_account_name": _as_str(item.get("receiver_account_name")),
        "date": _as_str(item.get("date")),
        "date_name": _as_str(item.get("date_name")),
        "created": _as_str(item.get("created")),
        "created_name": _as_str(item.get("created_name")),
        "created_by": _as_str(item.get("created_by")),
        "type": "sender",
        "description": _as_str(item.get("description")),
        "amount": item.get("amount"),
    }


def _map_receiver(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "transaction_id": _as_str(item.get("transaction_id")),
        "counterparty_account_number": _as_str(item.get("sender_account_number")),
        "counterparty_account_name": _as_str(item.get("sender_account_name")),
        "date": _as_str(item.get("date")),
        "date_name": _as_str(item.get("date_name")),
        "created": _as_str(item.get("created")),
        "created_name": _as_str(item.get("created_name")),
        "created_by": _as_str(item.get("created_by")),
        "type": "receiver",
        "description": _as_str(item.get("description")),
        "amount": item.get("amount"),
    }


def get_all_transactions_by_account_number(payload: Dict[str, Any]) -> Dict[str, Any]:
    account_number = _as_str(payload.get("account_number"))
    if not account_number:
        return {"exists": False, "message": "account_number is required", "transactions": []}

    sender_items = _query_all(IndexName=GSI_1_NAME, pk_name="gsi_1_pk", pk_value=account_number, scan_forward=False)
    receiver_items = _query_all(IndexName=GSI_2_NAME, pk_name="gsi_2_pk", pk_value=account_number, scan_forward=False)

    txs: List[Dict[str, Any]] = []
    txs.extend(_map_sender(it) for it in sender_items)
    txs.extend(_map_receiver(it) for it in receiver_items)

    txs.sort(key=lambda x: _as_str(x.get("created")), reverse=True)

    exists = len(txs) > 0
    msg = "ok" if exists else "no transactions found"

    return {
        "exists": exists,
        "message": msg,
        "transactions": txs,
    }


# ---- Example usage ----
payload = {"account_number": "1001"}
resp = get_all_transactions_by_account_number(payload)
print(json.dumps(resp, ensure_ascii=False, indent=2, default=_json_default))

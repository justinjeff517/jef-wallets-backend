import os
import json
import time
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from typing import Any, Dict, List

import boto3
from boto3.dynamodb.conditions import Key


# ----------------------------
# CONFIG
# ----------------------------
AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
TABLE_NAME = (os.getenv("WALLETS_LEDGERS_TABLE") or "jef-wallets-ledgers").strip()

GSI_1_NAME = (os.getenv("WALLETS_LEDGERS_GSI1_NAME") or "gsi_1").strip()  # sender
GSI_2_NAME = (os.getenv("WALLETS_LEDGERS_GSI2_NAME") or "gsi_2").strip()  # receiver

# RCU limiter (free-tier style)
RCU_PER_SEC = int((os.getenv("DDB_RCU_PER_SEC") or "22").strip() or "22")
RCU_SAFETY = float((os.getenv("DDB_RCU_SAFETY") or "0.9").strip() or "0.9")
_RCU_BUDGET = max(1.0, float(RCU_PER_SEC) * float(RCU_SAFETY))

_rcu_window_start = time.time()
_rcu_used = 0.0


# ----------------------------
# CLIENTS (reuse across invocations)
# ----------------------------
_dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
_table = _dynamodb.Table(TABLE_NAME)


# ----------------------------
# UTIL
# ----------------------------
def _rcu_take(units: float) -> None:
    global _rcu_window_start, _rcu_used
    if units <= 0:
        return

    now = time.time()
    elapsed = now - _rcu_window_start
    if elapsed >= 1.0:
        _rcu_window_start = now
        _rcu_used = 0.0

    if _rcu_used + units <= _RCU_BUDGET:
        _rcu_used += units
        return

    sleep_s = max(0.0, 1.0 - elapsed)
    if sleep_s > 0:
        time.sleep(sleep_s)

    _rcu_window_start = time.time()
    _rcu_used = units


def _sum_consumed_capacity(resp: Dict[str, Any]) -> float:
    cc = resp.get("ConsumedCapacity")
    if not cc:
        return 0.0

    if isinstance(cc, dict):
        try:
            return float(cc.get("CapacityUnits") or 0.0)
        except Exception:
            return 0.0

    if isinstance(cc, list):
        total = 0.0
        for x in cc:
            if isinstance(x, dict):
                try:
                    total += float(x.get("CapacityUnits") or 0.0)
                except Exception:
                    pass
        return total

    return 0.0


def _as_str(v: Any) -> str:
    return v.strip() if isinstance(v, str) else ""


def _to_dec(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if v is None:
        return Decimal("0")
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def _money_num(v: Any) -> float:
    d = _to_dec(v).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(d)


def _parse_iso(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return datetime.min


def _query_all(index_name: str, pk_field: str, account_number: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    lek = None

    while True:
        kwargs = {
            "IndexName": index_name,
            "KeyConditionExpression": Key(pk_field).eq(account_number),
            "ReturnConsumedCapacity": "TOTAL",
        }
        if lek:
            kwargs["ExclusiveStartKey"] = lek

        resp = _table.query(**kwargs)
        _rcu_take(_sum_consumed_capacity(resp))

        out.extend(resp.get("Items") or [])
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break

    return out


def _batch_get_by_pk(pks: List[str]) -> List[Dict[str, Any]]:
    if not pks:
        return []

    client = _table.meta.client
    uniq = sorted(set([p for p in pks if p]))
    items: List[Dict[str, Any]] = []

    for i in range(0, len(uniq), 100):
        part = uniq[i : i + 100]
        req = {TABLE_NAME: {"Keys": [{"pk": pk} for pk in part]}}

        while True:
            resp = client.batch_get_item(RequestItems=req, ReturnConsumedCapacity="TOTAL")
            _rcu_take(_sum_consumed_capacity(resp))

            items.extend(((resp.get("Responses") or {}).get(TABLE_NAME)) or [])

            unp = (resp.get("UnprocessedKeys") or {}).get(TABLE_NAME)
            if not unp or not (unp.get("Keys") or []):
                break
            req = {TABLE_NAME: unp}

    return items


def build_double_entry_json(payload: Dict[str, Any]) -> Dict[str, Any]:
    account_number = _as_str(payload.get("account_number"))
    if not account_number:
        return {
            "exists": False,
            "message": "account_number is required",
            "account_number": "",
            "entries": [],
            "summary": {"count": 0, "debit_total": 0.0, "credit_total": 0.0},
        }

    sender_rows = _query_all(GSI_1_NAME, "gsi_1_pk", account_number)
    receiver_rows = _query_all(GSI_2_NAME, "gsi_2_pk", account_number)

    want_pks: List[str] = []
    for it in sender_rows:
        txid = _as_str(it.get("transaction_id"))
        if txid:
            want_pks.append(f"{txid}#credit")

    for it in receiver_rows:
        txid = _as_str(it.get("transaction_id"))
        if txid:
            want_pks.append(f"{txid}#debit")

    counterpart_rows = _batch_get_by_pk(want_pks)

    all_rows = sender_rows + receiver_rows + counterpart_rows

    by_tx: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for it in all_rows:
        txid = _as_str(it.get("transaction_id"))
        typ = _as_str(it.get("type"))
        if not txid or typ not in ("debit", "credit"):
            continue
        by_tx.setdefault(txid, {})[typ] = it

    entries: List[Dict[str, Any]] = []
    debit_total = Decimal("0")
    credit_total = Decimal("0")

    for txid, pair in by_tx.items():
        debit = pair.get("debit")
        credit = pair.get("credit")
        is_complete = bool(debit and credit)

        ref = debit or credit or {}
        created = _as_str(ref.get("created"))
        created_name = _as_str(ref.get("created_name"))
        date = _as_str(ref.get("date"))
        date_name = _as_str(ref.get("date_name"))
        description = _as_str(ref.get("description"))
        amount_dec = _to_dec(ref.get("amount")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        lines: List[Dict[str, Any]] = []
        if debit:
            lines.append(
                {
                    "type": "debit",
                    "account_number": _as_str(debit.get("sender_account_number")),
                    "account_name": _as_str(debit.get("sender_account_name")),
                    "amount": _money_num(debit.get("amount")),
                }
            )
        if credit:
            lines.append(
                {
                    "type": "credit",
                    "account_number": _as_str(credit.get("receiver_account_number")),
                    "account_name": _as_str(credit.get("receiver_account_name")),
                    "amount": _money_num(credit.get("amount")),
                }
            )

        for ln in lines:
            if ln["type"] == "debit":
                debit_total += amount_dec
            else:
                credit_total += amount_dec

        entries.append(
            {
                "transaction_id": txid,
                "created": created,
                "created_name": created_name,
                "date": date,
                "date_name": date_name,
                "description": description,
                "amount": float(amount_dec),
                "is_complete": is_complete,
                "lines": lines,
            }
        )

    entries.sort(key=lambda e: _parse_iso(_as_str(e.get("created"))), reverse=True)

    return {
        "exists": len(entries) > 0,
        "message": "ok" if entries else "no entries found",
        "account_number": account_number,
        "entries": entries,
        "summary": {
            "count": len(entries),
            "debit_total": float(debit_total),
            "credit_total": float(credit_total),
        },
    }


def _json_body(event: Dict[str, Any]) -> Dict[str, Any]:
    body = event.get("body")

    if isinstance(body, str) and body.strip():
        try:
            return json.loads(body)
        except Exception:
            return {}

    if isinstance(event, dict) and isinstance(event.get("account_number"), str):
        return event

    return {}


def lambda_handler(event, context):
    try:
        payload = _json_body(event if isinstance(event, dict) else {})
        result = build_double_entry_json(payload)

        return {
            "statusCode": 200,
            "headers": {
                "content-type": "application/json; charset=utf-8",
                "access-control-allow-origin": "*",
            },
            "body": json.dumps(result, ensure_ascii=False),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "content-type": "application/json; charset=utf-8",
                "access-control-allow-origin": "*",
            },
            "body": json.dumps(
                {"exists": False, "message": "internal_error", "account_number": "", "entries": [], "summary": {"count": 0, "debit_total": 0.0, "credit_total": 0.0}, "error": str(e)},
                ensure_ascii=False,
            ),
        }

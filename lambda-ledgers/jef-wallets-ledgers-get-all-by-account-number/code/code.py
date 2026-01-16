import os
import json
import time
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key


AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
TABLE_NAME = (os.getenv("WALLETS_LEDGERS_TABLE") or "jef-wallets-ledgers").strip()

GSI_1_NAME = (os.getenv("WALLETS_LEDGERS_GSI1_NAME") or "gsi_1").strip()  # sender
GSI_2_NAME = (os.getenv("WALLETS_LEDGERS_GSI2_NAME") or "gsi_2").strip()  # receiver

# ----------------------------
# RCU LIMITER (free-tier style)
# ----------------------------
RCU_PER_SEC = int((os.getenv("DDB_RCU_PER_SEC") or "22").strip() or "22")
RCU_SAFETY = float((os.getenv("DDB_RCU_SAFETY") or "0.9").strip() or "0.9")
_RCU_BUDGET = max(1.0, float(RCU_PER_SEC) * float(RCU_SAFETY))

_rcu_window_start = time.time()
_rcu_used = 0.0


def _rcu_take(units: float) -> None:
    """Simple per-second RCU limiter using actual ConsumedCapacity units."""
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

    # query(): dict with CapacityUnits
    if isinstance(cc, dict):
        try:
            return float(cc.get("CapacityUnits") or 0.0)
        except Exception:
            return 0.0

    # batch_get_item(): list of dicts with CapacityUnits
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


dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(TABLE_NAME)


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

        resp = table.query(**kwargs)
        _rcu_take(_sum_consumed_capacity(resp))

        out.extend(resp.get("Items") or [])
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
    return out


def _batch_get_by_pk(pks: List[str]) -> List[Dict[str, Any]]:
    if not pks:
        return []

    client = table.meta.client
    uniq = sorted(set([p for p in pks if p]))
    items: List[Dict[str, Any]] = []

    for i in range(0, len(uniq), 100):
        part = uniq[i : i + 100]
        req = {
            TABLE_NAME: {
                "Keys": [{"pk": pk} for pk in part],
            }
        }

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
            "summary": {"count": 0, "complete_count": 0, "debit_total": 0.0, "credit_total": 0.0, "net_for_account": 0.0},
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
    net_for_account = Decimal("0")
    complete_count = 0

    for txid, pair in by_tx.items():
        debit = pair.get("debit")
        credit = pair.get("credit")
        is_complete = bool(debit and credit)
        if is_complete:
            complete_count += 1

        ref = debit or credit or {}
        created = _as_str(ref.get("created"))
        created_name = _as_str(ref.get("created_name"))
        date = _as_str(ref.get("date"))
        date_name = _as_str(ref.get("date_name"))
        description = _as_str(ref.get("description"))
        amount_dec = _to_dec(ref.get("amount")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        lines: List[Dict[str, Any]] = []
        if debit:
            lines.append({
                "type": "debit",
                "account_number": _as_str(debit.get("sender_account_number")),
                "account_name": _as_str(debit.get("sender_account_name")),
                "amount": _money_num(debit.get("amount")),
            })
        if credit:
            lines.append({
                "type": "credit",
                "account_number": _as_str(credit.get("receiver_account_number")),
                "account_name": _as_str(credit.get("receiver_account_name")),
                "amount": _money_num(credit.get("amount")),
            })

        for ln in lines:
            if ln["type"] == "debit":
                debit_total += amount_dec
                if _as_str(ln["account_number"]) == account_number:
                    net_for_account -= amount_dec
            else:
                credit_total += amount_dec
                if _as_str(ln["account_number"]) == account_number:
                    net_for_account += amount_dec

        entries.append({
            "transaction_id": txid,
            "created": created,
            "created_name": created_name,
            "date": date,
            "date_name": date_name,
            "description": description,
            "amount": float(amount_dec),
            "is_complete": is_complete,
            "lines": lines,
        })

    entries.sort(key=lambda e: _parse_iso(_as_str(e.get("created"))), reverse=True)

    out = {
        "exists": len(entries) > 0,
        "message": "ok" if entries else "no entries found",
        "account_number": account_number,
        "entries": entries,
        "summary": {
            "count": len(entries),
            "complete_count": complete_count,
            "debit_total": float(debit_total),
            "credit_total": float(credit_total),
            "net_for_account": float(net_for_account),
        },
    }
    return out


payload = {"account_number": "1001"}
result = build_double_entry_json(payload)
print(json.dumps(result, ensure_ascii=False, indent=2))

import os
import boto3
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.exceptions import ClientError

AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
LEDGERS_TABLE = (os.getenv("LEDGERS_TABLE") or "jef-wallets-ledgers").strip()

_TZ_MANILA = timezone(timedelta(hours=8))

ddb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
table = ddb.Table(LEDGERS_TABLE)

def _clean(s: str) -> str:
    s = (s or "").strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s

def _as_decimal(n) -> Decimal:
    if isinstance(n, Decimal):
        return n
    if isinstance(n, (int, float, str)):
        try:
            return Decimal(str(n))
        except Exception:
            return Decimal("0")
    return Decimal("0")

def _fmt_date_name(dt: datetime) -> str:
    s = dt.astimezone(_TZ_MANILA).strftime("%B %d, %Y, %A")
    return s.replace(", 0", ", ").replace(" 0", " ")

def _fmt_created_name(dt: datetime) -> str:
    s = dt.astimezone(_TZ_MANILA).strftime("%B %d, %Y, %A, %I:%M %p")
    return s.replace(", 0", ", ").replace(" 0", " ").replace(":00 ", " ")

def _get_latest_balance(entity_number: str) -> Decimal:
    resp = table.query(
        KeyConditionExpression=Key("pk").eq(entity_number),
        ScanIndexForward=False,
        Limit=1,
    )
    items = resp.get("Items") or []
    if not items:
        return Decimal("0")
    return _as_decimal(items[0].get("balance_after", 0))

def _get_next_ledger_number(entity_number: str) -> str:
    resp = table.query(
        KeyConditionExpression=Key("pk").eq(entity_number),
        ScanIndexForward=False,
        Limit=1,
    )
    items = resp.get("Items") or []
    if not items:
        return "0000000001"

    last_sk = str(items[0].get("sk") or "")
    last_ln = ""
    if "#" in last_sk:
        last_ln = last_sk.split("#", 1)[1].strip()

    try:
        n = int(last_ln) + 1
    except Exception:
        n = 1

    return str(n).zfill(10)

def _find_existing_by_idempotency(entity_number: str, idempotency_key: str):
    resp = table.query(
        KeyConditionExpression=Key("pk").eq(entity_number),
        ScanIndexForward=False,
        Limit=50,
    )
    items = resp.get("Items") or []
    for it in items:
        if _clean(it.get("idempotency_key", "")) == idempotency_key:
            return it
    return None

def create_one_ledger(payload: dict) -> dict:
    entity_number = _clean(payload.get("entity_number", ""))
    sender_entity_number = _clean(payload.get("sender_entity_number", ""))
    sender_entity_name = _clean(payload.get("sender_entity_name", ""))
    receiver_entity_number = _clean(payload.get("receiver_entity_number", ""))
    receiver_entity_name = _clean(payload.get("receiver_entity_name", ""))

    typ = _clean(payload.get("type", "")).lower()
    description = _clean(payload.get("description", ""))
    amount = _as_decimal(payload.get("amount", 0))
    idempotency_key = _clean(payload.get("idempotency_key", ""))

    if not entity_number:
        return {"is_created": False, "message": "entity_number is required"}
    if not sender_entity_number or not sender_entity_name:
        return {"is_created": False, "message": "sender_entity_number and sender_entity_name are required"}
    if not receiver_entity_number or not receiver_entity_name:
        return {"is_created": False, "message": "receiver_entity_number and receiver_entity_name are required"}
    if typ not in ("credit", "debit"):
        return {"is_created": False, "message": "type must be 'credit' or 'debit'"}
    if not description:
        return {"is_created": False, "message": "description is required"}
    if amount <= 0:
        return {"is_created": False, "message": "amount must be > 0"}
    if not idempotency_key:
        return {"is_created": False, "message": "idempotency_key is required (uuidv4)"}

    # idempotent check (best-effort; assumes same entity_number)
    existing = _find_existing_by_idempotency(entity_number, idempotency_key)
    if existing:
        ln = _clean(existing.get("ledger_number", ""))
        return {"is_created": True, "message": f"Already created ledger {ln} (idempotency_key matched)"}

    now = datetime.now(_TZ_MANILA)
    created = now.isoformat(timespec="seconds")
    date = now.strftime("%Y-%m-%d")

    balance_before = _get_latest_balance(entity_number)
    balance_after = balance_before + amount if typ == "credit" else balance_before - amount

    ledger_number = _get_next_ledger_number(entity_number)
    pk = entity_number
    sk = f"{created}#{ledger_number}"

    item = {
        "pk": pk,
        "sk": sk,
        "entity_number": entity_number,

        "sender_entity_number": sender_entity_number,
        "sender_entity_name": sender_entity_name,
        "receiver_entity_number": receiver_entity_number,
        "receiver_entity_name": receiver_entity_name,

        "ledger_number": ledger_number,
        "date": date,
        "date_name": _fmt_date_name(now),
        "created": created,
        "created_name": _fmt_created_name(now),
        "created_by": "00000",

        "type": typ,
        "description": description,
        "balance_before": balance_before,
        "amount": amount,
        "balance_after": balance_after,

        "idempotency_key": idempotency_key,
    }

    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
        )
        return {"is_created": True, "message": f"Created ledger {ledger_number} (balance_after={balance_after})"}
    except ClientError as e:
        code = (e.response.get("Error") or {}).get("Code") or ""
        if code == "ConditionalCheckFailedException":
            # retry-safe: check again by idempotency_key
            existing2 = _find_existing_by_idempotency(entity_number, idempotency_key)
            if existing2:
                ln = _clean(existing2.get("ledger_number", ""))
                return {"is_created": True, "message": f"Already created ledger {ln} (idempotency_key matched)"}
            return {"is_created": False, "message": "Ledger already exists (pk+sk collision). Retry."}
        return {"is_created": False, "message": f"DynamoDB error: {code}"}
    except Exception as e:
        return {"is_created": False, "message": f"Error: {e}"}

# --- Example ---
test_payload = {
    "entity_number": "1001",
    "sender_entity_number": "1001",
    "sender_entity_name": "Ellorimo Farm",
    "receiver_entity_number": "1002",
    "receiver_entity_name": "JEF Eggstore",
    "type": "debit",
    "description": "Feed purchase payment to JEF Eggstore",
    "amount": 850,
    "idempotency_key": "2f7a9c1e-0a6b-4a2d-8f1c-5c9b2d7a1e0f",
}


create_one_ledger(test_payload)
resp = create_one_ledger(test_payload)
print(resp)

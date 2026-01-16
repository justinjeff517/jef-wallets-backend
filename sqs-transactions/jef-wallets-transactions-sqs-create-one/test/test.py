import os
import json
from decimal import Decimal

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()

LAMBDA_ARN = (
    os.getenv("LAMBDA_ARN")
    or "arn:aws:lambda:ap-southeast-1:246715082475:function:jef-wallets-transactions-sqs-create-one"
).strip()

TIMEOUT_SECONDS = int(os.getenv("AWS_TIMEOUT_SECONDS") or 30)

client = boto3.client(
    "lambda",
    region_name=AWS_REGION,
    config=Config(
        retries={"max_attempts": 10, "mode": "standard"},
        connect_timeout=TIMEOUT_SECONDS,
        read_timeout=TIMEOUT_SECONDS,
    ),
)

def _to_decimal(obj):
    return json.loads(json.dumps(obj), parse_float=Decimal, parse_int=Decimal)

def _decode_payload(p):
    if p is None:
        return ""
    if isinstance(p, (bytes, bytearray)):
        return p.decode("utf-8", errors="replace")
    try:
        return p.read().decode("utf-8", errors="replace")
    except Exception:
        return str(p)

def invoke_lambda_http_style(payload: dict) -> dict:
    event = {
        "version": "2.0",
        "routeKey": "POST /",
        "rawPath": "/",
        "headers": {"content-type": "application/json"},
        "isBase64Encoded": False,
        "body": json.dumps(_to_decimal(payload), default=str),
    }

    try:
        resp = client.invoke(
            FunctionName=LAMBDA_ARN,
            InvocationType="RequestResponse",
            Payload=json.dumps(event).encode("utf-8"),
        )

        status_code = int(resp.get("StatusCode") or 0)
        fn_error = resp.get("FunctionError") or ""

        raw = _decode_payload(resp.get("Payload"))
        try:
            out = json.loads(raw) if raw else {}
        except Exception:
            out = {"raw": raw}

        return {
            "ok": (200 <= status_code < 300) and not fn_error,
            "status_code": status_code,
            "function_error": fn_error,
            "response": out,
        }

    except ClientError as e:
        return {
            "ok": False,
            "status_code": 0,
            "function_error": "ClientError",
            "response": {"message": e.response.get("Error", {}).get("Message", str(e))},
        }
    except Exception as e:
        return {
            "ok": False,
            "status_code": 0,
            "function_error": "Error",
            "response": {"message": str(e)},
        }

payload = {
    "account_number": "1001",
    "sender_account_number": "1001",
    "sender_account_name": "Ellorimo Farm",
    "receiver_account_number": "1002",
    "receiver_account_name": "JEF Eggstore",
    "description": "Daily egg sales remittance (afternoon batch)",
    "amount": 959,
    "transaction_id": "d4b1e9c0-6b2a-4c3f-9e1a-2a7b8c9d0959",
    "created_by": "00031",
}

print(invoke_lambda_http_style(payload))

import os
import json
from decimal import Decimal
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# -----------------------------
# CONFIG
# -----------------------------
AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
SQS_URL = (
    os.getenv("SQS_URL")
    or "https://sqs.ap-southeast-1.amazonaws.com/246715082475/jef-wallets-transactions-sqs-create-one"
).strip()

TIMEOUT_SECONDS = int(os.getenv("AWS_TIMEOUT_SECONDS") or 30)

sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION,
    config=Config(
        retries={"max_attempts": 10, "mode": "standard"},
        connect_timeout=TIMEOUT_SECONDS,
        read_timeout=TIMEOUT_SECONDS,
    ),
)


# -----------------------------
# HELPERS
# -----------------------------
def _to_decimal(obj):
    return json.loads(json.dumps(obj), parse_float=Decimal, parse_int=Decimal)


def _resp(code: int, body: dict):
    return {
        "statusCode": int(code),
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body, default=str),
    }


# -----------------------------
# LAMBDA HANDLER (HTTP / API GW / Function URL)
# -----------------------------
def lambda_handler(event, context):
    """
    Expects JSON body (API Gateway / Lambda Function URL style):
      {
        "account_number": "...",
        "sender_account_number": "...",
        "sender_account_name": "...",
        "receiver_account_number": "...",
        "receiver_account_name": "...",
        "description": "...",
        "amount": 123,
        "transaction_id": "uuid",
        "created_by": "00031"
      }

    Returns:
      { "is_sent": boolean, "message": string }
    """
    try:
        raw_body = event.get("body")
        if raw_body is None:
            return _resp(400, {"is_sent": False, "message": "Missing request body"})

        # If API Gateway sets isBase64Encoded
        if event.get("isBase64Encoded") is True:
            import base64

            raw_body = base64.b64decode(raw_body).decode("utf-8", errors="replace")

        try:
            payload = json.loads(raw_body) if isinstance(raw_body, str) else (raw_body or {})
        except Exception:
            return _resp(400, {"is_sent": False, "message": "Body must be valid JSON"})

        required = [
            "account_number",
            "sender_account_number",
            "sender_account_name",
            "receiver_account_number",
            "receiver_account_name",
            "description",
            "amount",
            "transaction_id",
            "created_by",
        ]
        missing = [k for k in required if k not in payload]
        if missing:
            return _resp(
                400,
                {"is_sent": False, "message": f"Missing required fields: {', '.join(missing)}"},
            )

        body = json.dumps(_to_decimal(payload), default=str)

        resp = sqs.send_message(
            QueueUrl=SQS_URL,
            MessageBody=body,
            MessageAttributes={
                "event_type": {"DataType": "String", "StringValue": "transactions.create_one"},
                "account_number": {"DataType": "String", "StringValue": str(payload["account_number"])},
            },
        )

        msg_id = resp.get("MessageId", "")
        return _resp(200, {"is_sent": True, "message": f"sent ok{(' ' + msg_id) if msg_id else ''}"})

    except ClientError as e:
        return _resp(
            500,
            {"is_sent": False, "message": f"AWS ClientError: {e.response.get('Error', {}).get('Message', str(e))}"},
        )
    except Exception as e:
        return _resp(500, {"is_sent": False, "message": f"Error: {str(e)}"})

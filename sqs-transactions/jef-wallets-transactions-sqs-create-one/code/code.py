import os
import json
from decimal import Decimal
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

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

def _to_decimal(obj):
    return json.loads(json.dumps(obj), parse_float=Decimal, parse_int=Decimal)

def send_transaction_message(payload: dict) -> dict:
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
        return {"is_sent": False, "message": f"Missing required fields: {', '.join(missing)}"}

    try:
        body = json.dumps(_to_decimal(payload), default=str)

        resp = sqs.send_message(
            QueueUrl=SQS_URL,
            MessageBody=body,
            # Optional: useful for FIFO ordering/dedup if you later switch to FIFO
            MessageAttributes={
                "event_type": {"DataType": "String", "StringValue": "transactions.create_one"},
                "account_number": {"DataType": "String", "StringValue": str(payload["account_number"])},
            },
        )

        msg_id = resp.get("MessageId", "")
        return {"is_sent": True, "message": f"sent ok{(' ' + msg_id) if msg_id else ''}"}

    except ClientError as e:
        return {"is_sent": False, "message": f"AWS ClientError: {e.response.get('Error', {}).get('Message', str(e))}"}
    except Exception as e:
        return {"is_sent": False, "message": f"Error: {str(e)}"}

# -----------------------------
# example usage
# -----------------------------
payload = {
    "account_number": "1001",
    "sender_account_number": "1001",
    "sender_account_name": "Ellorimo Farm",
    "receiver_account_number": "1002",
    "receiver_account_name": "JEF Eggstore",
    "description": "Daily egg sales remittance (afternoon batch)",
    "amount": 953,
    "transaction_id": "d4b1e9c0-6b2a-4c3f-9e1a-2a7b8c9d0953",
    "created_by": "00031",
}

result = send_transaction_message(payload)
print(result)

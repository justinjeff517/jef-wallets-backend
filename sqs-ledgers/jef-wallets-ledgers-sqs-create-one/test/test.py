import os
import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
LAMBDA_ARN = "arn:aws:lambda:ap-southeast-1:246715082475:function:jef-wallets-ledgers-sqs-create-one"

client = boto3.client(
    "lambda",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)

payload = {
    "account_number": "1001",
    "sender_account_number": "1001",
    "sender_account_name": "JEF Main",
    "receiver_account_number": "2001",
    "receiver_account_name": "Supplier A",
    "type": "debit",
    "description": "Payment for feed",
    "amount": 1250.75,
    "created_by": "00001",
    "ledger_id": "b6c4a6a8-0b2a-4dbe-8f66-8c8f8c8f8c8f",
}

def invoke_lambda(p: dict):
    try:
        resp = client.invoke(
            FunctionName=LAMBDA_ARN,
            InvocationType="RequestResponse",
            Payload=json.dumps(p).encode("utf-8"),
        )

        status = resp.get("StatusCode")
        fn_err = resp.get("FunctionError")

        raw = resp.get("Payload").read() if resp.get("Payload") else b""
        text = raw.decode("utf-8", errors="replace").strip()

        try:
            body = json.loads(text) if text else {}
        except Exception:
            body = {"raw": text}

        return {
            "StatusCode": status,
            "FunctionError": fn_err,
            "Response": body,
        }

    except ClientError as e:
        return {"error": "AWS ClientError", "message": e.response.get("Error", {}).get("Message", str(e))}
    except Exception as e:
        return {"error": "Exception", "message": str(e)}

print(json.dumps(invoke_lambda(payload), indent=2))

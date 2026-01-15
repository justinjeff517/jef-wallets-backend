import os
import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
LAMBDA_ARN = "arn:aws:lambda:ap-southeast-1:246715082475:function:jef-wallets-ledgers-get-all-by-account-number"

client = boto3.client(
    "lambda",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)

# Use ONE of these payloads, depending on what your Lambda expects:
payload = {
    # "entity_number": "1001",
    "account_number": "1001",
}

def invoke_lambda(payload: dict):
    try:
        resp = client.invoke(
            FunctionName=LAMBDA_ARN,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )

        status = resp.get("StatusCode")
        fn_err = resp.get("FunctionError")

        raw = resp["Payload"].read()
        text = raw.decode("utf-8", errors="replace").strip()

        try:
            body = json.loads(text) if text else None
        except Exception:
            body = text

        return {
            "status_code": status,
            "function_error": fn_err,
            "response": body,
            "raw": text,
        }

    except ClientError as e:
        return {"error": str(e), "payload": payload}

out = invoke_lambda(payload)
print(json.dumps(out, indent=2, ensure_ascii=False))

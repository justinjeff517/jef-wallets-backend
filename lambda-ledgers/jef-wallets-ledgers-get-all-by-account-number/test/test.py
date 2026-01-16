import os
import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()

# Update these for the NEW lambda
LAMBDA_ARN = (
    os.getenv("LAMBDA_ARN")
    or "arn:aws:lambda:ap-southeast-1:246715082475:function:jef-wallets-ledgers-get-all-by-account-number"
)

client = boto3.client(
    "lambda",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)

# NEW payload format
payload = {
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
            parsed = json.loads(text) if text else None
        except Exception:
            parsed = None

        # If the Lambda returns API Gateway proxy format, unwrap body
        response_obj = parsed
        if isinstance(parsed, dict) and "body" in parsed:
            body_raw = parsed.get("body")
            if isinstance(body_raw, str):
                try:
                    response_obj = json.loads(body_raw) if body_raw.strip() else None
                except Exception:
                    response_obj = body_raw
            else:
                response_obj = body_raw

        return {
            "status_code": status,
            "function_error": fn_err,
            "response": response_obj,
            "raw": text,
        }

    except ClientError as e:
        return {"error": str(e), "payload": payload}

out = invoke_lambda(payload)
print(json.dumps(out, indent=2, ensure_ascii=False))

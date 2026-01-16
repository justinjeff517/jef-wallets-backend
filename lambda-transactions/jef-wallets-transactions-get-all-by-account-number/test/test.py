import os
import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()

LAMBDA_ARN = (
    os.getenv("LAMBDA_ARN")
    or "arn:aws:lambda:ap-southeast-1:246715082475:function:jef-wallets-transactions-get-all-by-account-number"
).strip()

TIMEOUT_SECONDS = int(os.getenv("LAMBDA_TIMEOUT_SECONDS") or 30)

client = boto3.client(
    "lambda",
    region_name=AWS_REGION,
    config=Config(
        retries={"max_attempts": 10, "mode": "standard"},
        read_timeout=TIMEOUT_SECONDS,
        connect_timeout=TIMEOUT_SECONDS,
    ),
)


def _decode_payload(payload):
    if payload is None:
        return ""
    if isinstance(payload, (bytes, bytearray)):
        return payload.decode("utf-8", errors="replace")
    try:
        return payload.read().decode("utf-8", errors="replace")
    except Exception:
        return str(payload)


def invoke_test(account_number="1001", invocation_type="RequestResponse"):
    payload = {"account_number": str(account_number).strip()}
    print("Invoking:", LAMBDA_ARN)
    print("Payload:", json.dumps(payload, ensure_ascii=False))

    try:
        resp = client.invoke(
            FunctionName=LAMBDA_ARN,
            InvocationType=invocation_type,
            Payload=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        )
    except ClientError as e:
        print("Invoke failed:", e)
        raise

    status = resp.get("StatusCode")
    fn_err = resp.get("FunctionError")
    body_text = _decode_payload(resp.get("Payload"))

    print("StatusCode:", status)
    if fn_err:
        print("FunctionError:", fn_err)

    print("Raw response:")
    print(body_text)

    try:
        parsed = json.loads(body_text) if body_text else None
    except Exception:
        parsed = None

    if parsed is not None:
        print("Parsed JSON:")
        print(json.dumps(parsed, ensure_ascii=False, indent=2))
    return parsed


if __name__ == "__main__":
    invoke_test(account_number=os.getenv("TEST_ACCOUNT_NUMBER") or "1001")

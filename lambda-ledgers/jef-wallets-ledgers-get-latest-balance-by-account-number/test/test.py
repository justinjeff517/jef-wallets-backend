import os
import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

AWS_REGION = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1").strip()
LAMBDA_ARN = "arn:aws:lambda:ap-southeast-1:246715082475:function:jef-wallets-ledgers-get-latest-balance-by-account-number"

client = boto3.client(
    "lambda",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)

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

        raw_bytes = resp["Payload"].read()
        raw_text = raw_bytes.decode("utf-8", errors="replace").strip()

        try:
            body = json.loads(raw_text) if raw_text else {}
        except Exception:
            body = {"_raw": raw_text}

        print("Invoke StatusCode:", status)
        print("FunctionError:", fn_err)

        if isinstance(body, dict) and "body" in body and isinstance(body["body"], str):
            try:
                parsed_body = json.loads(body["body"])
                body["body"] = parsed_body
            except Exception:
                pass

        print(json.dumps(body, indent=2, ensure_ascii=False))
        return body

    except ClientError as e:
        print("ClientError:", e)
        raise
    except Exception as e:
        print("Error:", e)
        raise

if __name__ == "__main__":
    invoke_lambda(payload)

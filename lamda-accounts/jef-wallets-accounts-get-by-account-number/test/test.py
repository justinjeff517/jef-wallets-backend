import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

AWS_REGION = "ap-southeast-1"
LAMBDA_ARN = "arn:aws:lambda:ap-southeast-1:246715082475:function:jef-wallets-accounts-get-by-account-number"

_lambda = boto3.client(
    "lambda",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)

payload = {
    "account_number": "1001"
}

try:
    resp = _lambda.invoke(
        FunctionName=LAMBDA_ARN,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )

    raw = resp["Payload"].read()
    text = raw.decode("utf-8", errors="replace")

    try:
        out = json.loads(text) if text.strip() else {}
    except Exception:
        out = {"raw": text}

    print(json.dumps(out, indent=2))

except ClientError as e:
    print("ClientError:", e.response.get("Error", {}).get("Message", str(e)))
except Exception as e:
    print("Error:", str(e))

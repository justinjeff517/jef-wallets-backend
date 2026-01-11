import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

AWS_REGION = "ap-southeast-1"
LAMBDA_ARN = "arn:aws:lambda:ap-southeast-1:246715082475:function:jef-wallets-accounts-get-all"

_lambda = boto3.client(
    "lambda",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)

def invoke_accounts_get_all():
    try:
        payload = {}  # no input needed
        resp = _lambda.invoke(
            FunctionName=LAMBDA_ARN,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )

        status = resp.get("StatusCode")
        fn_err = resp.get("FunctionError")

        raw = resp["Payload"].read()
        text = raw.decode("utf-8", errors="replace")

        print("Invoke StatusCode:", status)
        if fn_err:
            print("FunctionError:", fn_err)

        try:
            data = json.loads(text)
        except Exception:
            data = {"raw": text}

        print(json.dumps(data, indent=2))
        return data

    except ClientError as e:
        print("ClientError:", (e.response.get("Error") or {}).get("Message") or str(e))
        raise

if __name__ == "__main__":
    invoke_accounts_get_all()

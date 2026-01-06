import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

LAMBDA_ARN = "arn:aws:lambda:ap-southeast-1:246715082475:function:jef-wallets-get-all-ledgers-by-entity-number"
AWS_REGION = "ap-southeast-1"

_lambda = boto3.client(
    "lambda",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)

def invoke(entity_number: str):
    payload = {"entity_number": str(entity_number)}
    try:
        resp = _lambda.invoke(
            FunctionName=LAMBDA_ARN,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )

        status_code = resp.get("StatusCode")
        raw = resp["Payload"].read().decode("utf-8", errors="replace")

        # If your Lambda returns API-Gateway style {statusCode, body}, unwrap it.
        try:
            outer = json.loads(raw) if raw.strip() else {}
        except Exception:
            outer = None

        if isinstance(outer, dict) and "body" in outer:
            body = outer.get("body") or ""
            try:
                data = json.loads(body) if isinstance(body, str) else body
            except Exception:
                data = body
            return {"invoke_status_code": status_code, "lambda_response": outer, "data": data}

        # Otherwise, it's already the final JSON response
        try:
            data = json.loads(raw) if raw.strip() else {}
        except Exception:
            data = raw

        return {"invoke_status_code": status_code, "data": data}

    except ClientError as e:
        return {"invoke_status_code": None, "error": e.response.get("Error", {}).get("Message", str(e))}
    except Exception as e:
        return {"invoke_status_code": None, "error": str(e)}

if __name__ == "__main__":
    out = invoke("1003")
    print(json.dumps(out, ensure_ascii=False, indent=2))

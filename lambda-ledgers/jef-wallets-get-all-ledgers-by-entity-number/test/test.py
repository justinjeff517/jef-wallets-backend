import os
import json
import boto3
from botocore.exceptions import ClientError

AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"
LAMBDA_ARN = "arn:aws:lambda:ap-southeast-1:246715082475:function:jef-wallets-get-all-ledgers-by-entity-number"

lambda_client = boto3.client("lambda", region_name=AWS_REGION)

def invoke_direct(entity_number: str) -> dict:
    payload = {"entity_number": str(entity_number).strip() if entity_number is not None else ""}
    try:
        resp = lambda_client.invoke(
            FunctionName=LAMBDA_ARN,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )

        raw = resp["Payload"].read()
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)

        try:
            out = json.loads(text) if text else {}
        except Exception:
            out = {"_raw": text}

        return {
            "ok": True,
            "status_code": resp.get("StatusCode"),
            "function_error": resp.get("FunctionError"),
            "result": out,
        }

    except ClientError as e:
        return {
            "ok": False,
            "error": "ClientError",
            "message": e.response.get("Error", {}).get("Message", str(e)),
        }
    except Exception as e:
        return {"ok": False, "error": "Exception", "message": str(e)}

# --- tests ---
print("TEST valid:")
print(json.dumps(invoke_direct("1001"), ensure_ascii=False, indent=2))


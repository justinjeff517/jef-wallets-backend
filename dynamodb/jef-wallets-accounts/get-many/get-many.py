import json
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from decimal import Decimal

TABLE_NAME = "jef-wallets-accounts"

def json_default(o):
    if isinstance(o, Decimal):
        if o % 1 == 0:
            return int(o)
        return float(o)
    return str(o)

def get_all_items(table_name=TABLE_NAME):
    ddb = boto3.resource("dynamodb")
    table = ddb.Table(table_name)

    out = []
    start_key = None

    while True:
        req = {}
        if start_key:
            req["ExclusiveStartKey"] = start_key

        resp = table.scan(**req)

        out.extend(resp.get("Items", []))

        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break

    return out

def main():
    try:
        items = get_all_items()
        print(json.dumps({
            "ok": True,
            "table": TABLE_NAME,
            "count": len(items),
            "items": items
        }, indent=2, ensure_ascii=False, default=json_default))
    except ClientError as e:
        print(json.dumps({
            "ok": False,
            "table": TABLE_NAME,
            "error": e.response.get("Error", {})
        }, indent=2, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({
            "ok": False,
            "table": TABLE_NAME,
            "error": str(e)
        }, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()

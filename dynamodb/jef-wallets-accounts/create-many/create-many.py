import json
import os
import boto3
from botocore.exceptions import ClientError

TABLE_NAME = "jef-wallets-accounts"
JSON_PATH = r"H:\github12\jef-wallets-backend\dynamodb\jef-wallets-accounts\create-many\datas.json"

def describe_key_schema(table_name: str):
    ddb = boto3.client("dynamodb")
    resp = ddb.describe_table(TableName=table_name)
    ks = resp["Table"]["KeySchema"]  # [{'AttributeName': '...', 'KeyType': 'HASH'}, ...]
    pk = next(x["AttributeName"] for x in ks if x["KeyType"] == "HASH")
    sk = next((x["AttributeName"] for x in ks if x["KeyType"] == "RANGE"), None)
    return pk, sk

def load_items(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("datas.json must be a JSON array (list of items).")
    return data

def coalesce_key(item: dict, key_name: str):
    if key_name in item and str(item[key_name]).strip():
        return item

    # Common fix-ups
    if key_name == "account_number" and "pk" in item and str(item["pk"]).strip():
        item["account_number"] = str(item["pk"]).strip()
        return item

    if key_name == "pk" and "account_number" in item and str(item["account_number"]).strip():
        item["pk"] = str(item["account_number"]).strip()
        return item

    return item

def main():
    if not os.path.isfile(JSON_PATH):
        raise FileNotFoundError(f"File not found: {JSON_PATH}")

    pk_name, sk_name = describe_key_schema(TABLE_NAME)
    print("Detected key schema:", {"pk": pk_name, "sk": sk_name})

    items = load_items(JSON_PATH)

    # Fix and validate keys
    fixed = []
    for it in items:
        if not isinstance(it, dict):
            raise ValueError(f"Each item must be an object/dict. Got: {type(it)}")

        it = coalesce_key(it, pk_name)
        if sk_name:
            it = coalesce_key(it, sk_name)

        if pk_name not in it or not str(it[pk_name]).strip():
            raise ValueError(f"Missing partition key '{pk_name}' in item: {it}")
        if sk_name and (sk_name not in it or not str(it[sk_name]).strip()):
            raise ValueError(f"Missing sort key '{sk_name}' in item: {it}")

        # Ensure key values are strings if your schema expects strings
        it[pk_name] = str(it[pk_name]).strip()
        if sk_name:
            it[sk_name] = str(it[sk_name]).strip()

        fixed.append(it)

    ddb = boto3.resource("dynamodb")
    table = ddb.Table(TABLE_NAME)

    uploaded = 0
    with table.batch_writer(overwrite_by_pkeys=[pk_name] + ([sk_name] if sk_name else [])) as batch:
        for it in fixed:
            batch.put_item(Item=it)
            uploaded += 1

    print(json.dumps({
        "ok": True,
        "table": TABLE_NAME,
        "file": JSON_PATH,
        "attempted": len(items),
        "uploaded": uploaded,
        "key_schema": {"pk": pk_name, "sk": sk_name}
    }, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    try:
        main()
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

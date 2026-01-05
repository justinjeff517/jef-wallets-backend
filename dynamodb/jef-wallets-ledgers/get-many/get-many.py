import boto3
from boto3.dynamodb.types import TypeDeserializer

TABLE_NAME = "jef-wallets-ledgers"

ddb = boto3.client("dynamodb")
deser = TypeDeserializer()

def ddb_item_to_python(item):
    return {k: deser.deserialize(v) for k, v in item.items()}

def get_all_items(table_name=TABLE_NAME):
    out = []
    start_key = None

    while True:
        req = {"TableName": table_name}
        if start_key:
            req["ExclusiveStartKey"] = start_key

        resp = ddb.scan(**req)

        for it in resp.get("Items", []):
            out.append(ddb_item_to_python(it))

        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break

    return out

items = get_all_items()
print("count:", len(items))
print(items)

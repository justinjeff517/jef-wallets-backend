import os
import json
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr

TABLE_NAME = os.getenv("LEDGERS_TABLE") or "jef-wallets-ledgers"
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-1"

_dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    config=Config(retries={"max_attempts": 10, "mode": "standard"}),
)
_table = _dynamodb.Table(TABLE_NAME)

PK_NAME = "pk"
SK_NAME = None  # important: set to None

DELETE_ACCOUNT_NUMBER = os.getenv("DELETE_ACCOUNT_NUMBER", "").strip()

# --- WCU FREE TIER / THROTTLE ---
# Provisioned free-tier is commonly treated as 25 WCU available. We enforce a per-second WCU budget here.
WCU_LIMIT_PER_SECOND = float(os.getenv("WCU_LIMIT_PER_SECOND", "25") or 25)
# Optional headroom (e.g., set to 0.9 to use 90% of limit). Default uses full limit.
WCU_SAFETY_FACTOR = float(os.getenv("WCU_SAFETY_FACTOR", "1.0") or 1.0)
WCU_BUDGET = max(0.0, WCU_LIMIT_PER_SECOND * max(0.0, min(1.0, WCU_SAFETY_FACTOR)))

# Fallback WCU estimate if ConsumedCapacity isn't returned for some reason
FALLBACK_WCU_PER_DELETE = float(os.getenv("FALLBACK_WCU_PER_DELETE", "1") or 1)

DRY_RUN = (os.getenv("DRY_RUN", "0").strip() == "1")


def _throttle_if_needed(window_start_monotonic, window_wcu):
    if WCU_BUDGET <= 0:
        return time.monotonic(), 0.0

    now = time.monotonic()
    elapsed = now - window_start_monotonic

    # Reset window if we're past 1s
    if elapsed >= 1.0:
        return now, 0.0

    # If at/over budget, sleep until next window
    if window_wcu >= WCU_BUDGET:
        sleep_s = 1.0 - elapsed
        if sleep_s > 0:
            time.sleep(sleep_s)
        return time.monotonic(), 0.0

    return window_start_monotonic, window_wcu


def delete_all_ledgers():
    deleted_count = 0
    deleted_wcu_total = 0.0

    # WCU accounting window (per second)
    window_start = time.monotonic()
    window_wcu = 0.0

    try:
        last_evaluated_key = None

        scan_kwargs = {
            "ProjectionExpression": PK_NAME  # only need pk for delete
        }

        if DELETE_ACCOUNT_NUMBER:
            scan_kwargs["FilterExpression"] = Attr("account_number").eq(DELETE_ACCOUNT_NUMBER)

        while True:
            if last_evaluated_key:
                scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

            response = _table.scan(**scan_kwargs)
            items = response.get("Items", [])

            for item in items:
                pk_value = item.get(PK_NAME)
                if pk_value is None:
                    continue

                key = {PK_NAME: pk_value}

                # throttle BEFORE each delete based on current per-second WCU usage
                window_start, window_wcu = _throttle_if_needed(window_start, window_wcu)

                if DRY_RUN:
                    deleted_count += 1
                    # assume 0 WCU in dry run
                    continue

                dresp = _table.delete_item(
                    Key=key,
                    ReturnConsumedCapacity="TOTAL",
                )

                cc = dresp.get("ConsumedCapacity") or {}
                used = cc.get("CapacityUnits")
                try:
                    used_wcu = float(used) if used is not None else FALLBACK_WCU_PER_DELETE
                except Exception:
                    used_wcu = FALLBACK_WCU_PER_DELETE

                window_wcu += used_wcu
                deleted_wcu_total += used_wcu
                deleted_count += 1

                # throttle AFTER, in case the delete pushed us over budget
                window_start, window_wcu = _throttle_if_needed(window_start, window_wcu)

            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        if deleted_count == 0:
            msg = "No ledgers found."
            if DELETE_ACCOUNT_NUMBER:
                msg += f" (filtered by account_number={DELETE_ACCOUNT_NUMBER})"
            return {
                "is_deleted": False,
                "deleted_count": 0,
                "wcu_consumed_estimate": round(deleted_wcu_total, 4),
                "message": msg,
            }

        scope = f" for account_number={DELETE_ACCOUNT_NUMBER}" if DELETE_ACCOUNT_NUMBER else " (all items)"
        extra = " (dry run)" if DRY_RUN else ""
        return {
            "is_deleted": True,
            "deleted_count": deleted_count,
            "wcu_consumed_estimate": round(deleted_wcu_total, 4),
            "message": f"Deleted {deleted_count} ledger(s){scope}{extra}.",
        }

    except ClientError as e:
        msg = e.response.get("Error", {}).get("Message", str(e))
        return {
            "is_deleted": False,
            "deleted_count": deleted_count,
            "wcu_consumed_estimate": round(deleted_wcu_total, 4),
            "message": f"DynamoDB error: {msg}",
        }
    except Exception as e:
        return {
            "is_deleted": False,
            "deleted_count": deleted_count,
            "wcu_consumed_estimate": round(deleted_wcu_total, 4),
            "message": f"Unexpected error: {str(e)}",
        }


def lambda_handler(event, context=None):
    result = delete_all_ledgers()
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(result, ensure_ascii=False, default=str),
    }


if __name__ == "__main__":
    print(json.dumps(delete_all_ledgers(), indent=2, ensure_ascii=False, default=str))

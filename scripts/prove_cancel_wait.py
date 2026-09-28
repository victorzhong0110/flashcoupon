#!/usr/bin/env python3
"""轮询一张刚抢到的订单，直到取消并且 Redis 库存加回去。

用法: prove_cancel_wait.py <coupon_id> <user_id> <total_stock> <shard_count> <out_json>
时间都从 mysql / redis-cli 读，脚本不算延迟。
"""
import json
import subprocess
import sys
import time

coupon_id = int(sys.argv[1])
user_id = int(sys.argv[2])
total = int(sys.argv[3])
shards = int(sys.argv[4])
out_path = sys.argv[5]

deadline = time.time() + 45


def mysql(sql):
    p = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "mysql",
            "mysql",
            "-uroot",
            "-proot",
            "-N",
            "coupon",
            "-e",
            sql,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or p.stdout.strip())
    return p.stdout.strip()


def redis_sum():
    total_left = 0
    present = 0
    for i in range(shards):
        p = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "redis",
                "redis-cli",
                "GET",
                f"tb:coupon:{coupon_id}:stock:{i}",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        raw = p.stdout.strip()
        if raw and raw != "(nil)":
            total_left += int(raw)
            present += 1
    return total_left, present


def redis_users():
    p = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "redis",
            "redis-cli",
            "SCARD",
            f"tb:coupon:{coupon_id}:users",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    raw = p.stdout.strip()
    return int(raw) if raw else -1


row_seen = None
cancelled_seen = None
stock_back = None
last = ""
while time.time() < deadline:
    sql = (
        "SELECT status, stock_returned, "
        "UNIX_TIMESTAMP(expire_at), UNIX_TIMESTAMP(updated_at), id "
        f"FROM orders WHERE coupon_id={coupon_id} AND user_id={user_id} "
        "ORDER BY id DESC LIMIT 1"
    )
    try:
        last = mysql(sql)
    except RuntimeError as exc:
        last = f"mysql_error {exc}"
        time.sleep(0.05)
        continue
    now = time.time()
    if last:
        parts = last.split("\t")
        if row_seen is None:
            row_seen = now
        status, returned, expire_at, updated_at, order_id = parts
        if status == "CANCELLED" and cancelled_seen is None:
            cancelled_seen = now
        if status == "CANCELLED" and returned == "1":
            left, present = redis_sum()
            users = redis_users()
            if left == total and users == 0:
                stock_back = now
                result = {
                    "coupon_id": coupon_id,
                    "user_id": user_id,
                    "order_id": int(order_id),
                    "status": status,
                    "stock_returned": int(returned),
                    "expire_at_unix": float(expire_at),
                    "updated_at_unix": float(updated_at),
                    "redis_remaining": left,
                    "redis_shards_present": present,
                    "redis_users": users,
                    "total_stock": total,
                    "cancel_minus_expire_sec": float(updated_at) - float(expire_at),
                    "observed_stock_back_minus_expire_sec": stock_back - float(expire_at),
                    "row_seen_minus_expire_sec": (row_seen - float(expire_at)) if row_seen else None,
                    "ok": True,
                }
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(result, f, indent=2)
                    f.write("\n")
                print(json.dumps(result))
                sys.exit(0)
    time.sleep(0.05)

fail = {
    "coupon_id": coupon_id,
    "user_id": user_id,
    "ok": False,
    "last_row": last,
    "row_seen": row_seen is not None,
    "cancelled_seen": cancelled_seen is not None,
}
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(fail, f, indent=2)
    f.write("\n")
print(json.dumps(fail))
sys.exit(1)

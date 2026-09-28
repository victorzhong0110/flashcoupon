#!/usr/bin/env python3
"""把 k6 --out json 收成按秒的计数，原始点文件不进 git。"""
import collections
import json
import sys

counts = collections.defaultdict(lambda: collections.Counter())
durs = collections.defaultdict(list)
keep = {
    "grab_success",
    "grab_replay",
    "grab_sold_out",
    "grab_already",
    "grab_limited",
    "grab_failed",
    "http_reqs",
    "dropped_iterations",
}

for line in sys.stdin:
    line = line.strip()
    if not line or not line.startswith("{"):
        continue
    try:
        ev = json.loads(line)
    except json.JSONDecodeError:
        continue
    if ev.get("type") != "Point":
        continue
    metric = ev.get("metric")
    data = ev.get("data") or {}
    ts = str(data.get("time") or "")
    if len(ts) < 19:
        continue
    sec = ts[:19]
    if metric == "http_req_duration":
        durs[sec].append(float(data.get("value") or 0))
        continue
    if metric in keep:
        counts[sec][metric] += float(data.get("value") or 0)

def pct(xs, p):
    if not xs:
        return ""
    xs = sorted(xs)
    i = min(len(xs) - 1, max(0, int(round((p / 100.0) * (len(xs) - 1)))))
    return f"{xs[i]:.2f}"

print("second,http_reqs,grab_success,grab_replay,grab_sold_out,grab_already,grab_limited,grab_failed,dropped_iterations,p99_ms")
for sec in sorted(counts.keys() | durs.keys()):
    c = counts[sec]
    print(
        ",".join(
            [
                sec,
                f"{c['http_reqs']:.0f}",
                f"{c['grab_success']:.0f}",
                f"{c['grab_replay']:.0f}",
                f"{c['grab_sold_out']:.0f}",
                f"{c['grab_already']:.0f}",
                f"{c['grab_limited']:.0f}",
                f"{c['grab_failed']:.0f}",
                f"{c['dropped_iterations']:.0f}",
                pct(durs[sec], 99),
            ]
        )
    )

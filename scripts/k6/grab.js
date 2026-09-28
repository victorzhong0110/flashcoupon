// 抢券压测。每个迭代使用不同的用户和幂等键，避免“一人一单”把结果变成纯拒绝流量。
// 库存要大于总请求数，测到的才是扣减路径，而不是售罄快速失败。
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const success = new Counter("grab_success");
const soldOut = new Counter("grab_sold_out");
const already = new Counter("grab_already");
const limited = new Counter("grab_limited");
const failed = new Counter("grab_failed");

export const options = {
  // med 是 P50。默认汇总没有 P99，这里显式加上，方便写进压测报告。
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
  scenarios: {
    grab: {
      executor: "constant-vus",
      vus: Number(__ENV.VUS || 50),
      duration: __ENV.DURATION || "15s",
    },
  },
};

const base = __ENV.BASE_URL;
const coupon = __ENV.COUPON_ID;
// 每次 k6 进程用一个前缀，重复跑不会和上一轮的一人一单撞上。
const prefix = __ENV.PREFIX || "k";

export default function () {
  // USER_BASE 让不同轮次的用户错开。同一张券上重复压测时，不能复用上一轮的 user id，
  // 否则一人一单会把后半段变成 already_owned，QPS 就不再是扣库存路径。
  const user = Number(__ENV.USER_BASE || 1) + __VU * 1000000 + __ITER;
  const res = http.post(`${base}/v1/coupons/${coupon}/grab`, null, {
    headers: {
      "X-User-Id": String(user),
      "Idempotency-Key": `${prefix}-b${__ENV.USER_BASE || 0}-v${__VU}-i${__ITER}`,
    },
    timeout: "30s",
  });
  const body = res.body || "";
  if (res.status === 200) {
    success.add(1);
  } else if (res.status === 409 && body.indexOf("sold_out") >= 0) {
    soldOut.add(1);
  } else if (res.status === 409 && body.indexOf("already_owned") >= 0) {
    already.add(1);
  } else if (res.status === 429) {
    limited.add(1);
  } else {
    failed.add(1);
  }
  check(res, {
    "not 5xx": (r) => r.status < 500,
  });
}

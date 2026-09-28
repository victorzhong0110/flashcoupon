// 接近开卖的流量：开头几秒把到达率拉到每秒两千，后面留下一条长尾。
// 券按 Zipf 抽取（下标 0 最热）。请求里混着普通用户、机器人连点和重复点击。
//
// 计数分开，避免把售罄、回放、限流算进 goodput。
// goodput = grab_success 的速率（HTTP 200 且 replay=false），单位是每秒新预扣。
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const success = new Counter("grab_success");
const replay = new Counter("grab_replay");
const soldOut = new Counter("grab_sold_out");
const already = new Counter("grab_already");
const limited = new Counter("grab_limited");
const failed = new Counter("grab_failed");
const personaFresh = new Counter("persona_fresh");
const personaBot = new Counter("persona_bot");
const personaReplay = new Counter("persona_replay");
const personaRepeat = new Counter("persona_repeat");

const coupons = String(__ENV.COUPON_IDS || "")
  .split(",")
  .map((s) => s.trim())
  .filter((s) => s.length > 0);

// s=1.1 时最热的一张大约拿走三成多的普通流量。CDF 在 init 里算一次。
const zipfS = Number(__ENV.ZIPF_S || 1.1);
let zipfCdf = [];

export const options = {
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
  scenarios: {
    spike: {
      executor: "ramping-arrival-rate",
      startRate: Number(__ENV.START_RATE || 50),
      timeUnit: "1s",
      preAllocatedVUs: Number(__ENV.PRE_VUS || 300),
      maxVUs: Number(__ENV.MAX_VUS || 700),
      stages: [
        { duration: __ENV.RAMP || "2s", target: Number(__ENV.SPIKE_RPS || 2000) },
        { duration: __ENV.HOLD || "3s", target: Number(__ENV.SPIKE_RPS || 2000) },
        { duration: __ENV.FALL || "2s", target: Number(__ENV.TAIL_RPS || 300) },
        { duration: __ENV.TAIL || "8s", target: Number(__ENV.TAIL_RPS || 300) },
      ],
    },
  },
};

const base = __ENV.BASE_URL;
const prefix = __ENV.PREFIX || "r";
const userBase = Number(__ENV.USER_BASE || 1);
const botBase = Number(__ENV.BOT_BASE || 910000000000);
const replayBase = Number(__ENV.REPLAY_BASE || 920000000000);

function buildZipf(n, s) {
  const w = [];
  let sum = 0;
  for (let i = 1; i <= n; i++) {
    const p = 1 / Math.pow(i, s);
    w.push(p);
    sum += p;
  }
  const cdf = [];
  let acc = 0;
  for (let i = 0; i < w.length; i++) {
    acc += w[i] / sum;
    cdf.push(acc);
  }
  return cdf;
}

function zipfIndex() {
  const u = Math.random();
  for (let i = 0; i < zipfCdf.length; i++) {
    if (u <= zipfCdf[i]) {
      return i;
    }
  }
  return zipfCdf.length - 1;
}

export function setup() {
  if (coupons.length < 2) {
    throw new Error("COUPON_IDS needs at least 2 ids, hottest first");
  }
  return { cdf: buildZipf(coupons.length, zipfS) };
}

export default function (data) {
  zipfCdf = data.cdf;
  const roll = Math.random();
  let user;
  let idem;
  let coupon;
  if (roll < 0.6) {
    personaFresh.add(1);
    user = userBase + __VU * 1000000 + __ITER;
    coupon = coupons[zipfIndex()];
    idem = `${prefix}-f-b${userBase}-v${__VU}-i${__ITER}`;
  } else if (roll < 0.8) {
    // 机器人：很少的账号死磕最热的券，每次换幂等键。
    personaBot.add(1);
    user = botBase + (__VU % 32);
    coupon = coupons[0];
    idem = `${prefix}-bot-b${userBase}-v${__VU}-i${__ITER}`;
  } else if (roll < 0.9) {
    // 重复点击里的超时重试：幂等键不变，应该回放，不再扣库存。
    personaReplay.add(1);
    user = replayBase + (__VU % 80);
    coupon = coupons[0];
    idem = `${prefix}-rp-b${userBase}-u${user}`;
  } else {
    // 同一个人换一个新幂等键再点，应该是 already_owned。
    personaRepeat.add(1);
    user = replayBase + (__VU % 80);
    coupon = coupons[0];
    idem = `${prefix}-re-b${userBase}-v${__VU}-i${__ITER}`;
  }

  const res = http.post(`${base}/v1/coupons/${coupon}/grab`, null, {
    headers: {
      "X-User-Id": String(user),
      "Idempotency-Key": idem,
    },
    timeout: "5s",
    tags: { persona: roll < 0.6 ? "fresh" : roll < 0.8 ? "bot" : roll < 0.9 ? "replay" : "repeat" },
  });
  const body = res.body || "";
  if (res.status === 200 && body.indexOf('"replay":true') >= 0) {
    replay.add(1);
  } else if (res.status === 200) {
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

# 压测记录

数字全部来自同目录的 k6 `summary-export` 和对账程序 JSON，没有手填。QPS 和延迟保留两位小数，笔数用整数。原始值以 JSON 为准。`http_req_duration` 的单位是毫秒，`med` 是 P50。k6 脚本显式打开了 `p(95)` 和 `p(99)`（`scripts/k6/grab.js`）。

## 机器

采集时间 `2026-09-28T07:12:16Z`，原文在 `docs/bench-results/machine.txt`。后面的对照、超卖证明和故障注入都在这台机器上，k6、API、消费者、Redis、MySQL、Prometheus 挤在同一台虚拟机里，没有单独的压测机。

| 项 | 值 |
|---|---|
| 系统 | Linux 6.12.94+，x86_64，KVM |
| CPU | Intel Xeon，4 vCPU，每核 1 线程，L3 320 MiB |
| 内存 | 15 GiB，无 swap。采集当时 used 9.6 GiB、available 6.1 GiB |
| Go | go1.22.2 linux/amd64 |
| k6 | v0.57.0 |
| Docker | 29.1.3 |
| Redis | 7.4.11（`appendfsync everysec`） |
| MySQL | 8.0.46 |

## 怎么读这组数

- 三种策略：`db` 在事务里 `UPDATE remaining WHERE remaining>0` 再插入订单；`redis` 一个库存 key，Lua 预扣后异步落库；`sharded` 同一条 Lua，库存拆成 16 个 key。
- 容量对照用的券：18（`db`，库存 300000）、19（`redis`，库存 3000000）、20（`sharded`，16 桶，库存 3000000）。见 `final-coupons.txt`。
- 每轮 `constant-vus`，`DURATION=12s`。请求数除以 k6 的 rate 大约是 12.0–12.1 秒。
- 每轮 `USER_BASE` 和幂等键都错开。成功笔数等于 HTTP 200 的笔数，`grab_sold_out` / `grab_already` / `grab_limited` / `grab_failed` 在这些 JSON 里没有计数（一次都没发生）。`checks` 的 “not 5xx” 失败数是 0。
- 全局限流 `GLOBAL_RPS=0`（关闭）。用户限流是 30 次/10 秒，但这组请求每个用户只打一次，限流器不在热路径上。
- `strategy=redis/sharded` 时，MySQL 的 `coupons.remaining` 不会跟着扣，它只对 `db` 策略有意义。对账等式用的是 Redis 剩余。

## 容量对照

同一台机器、同一套进程、库存都大于请求数，所以下面测的是扣减路径，不是售罄快速失败。

| 策略 | VU | 请求数 | QPS | P50 (ms) | P95 (ms) | P99 (ms) |
|---|---:|---:|---:|---:|---:|---:|
| db | 20 | 4297 | 356.19 | 55.28 | 73.86 | 89.52 |
| db | 50 | 4192 | 346.09 | 142.71 | 171.28 | 182.38 |
| redis | 50 | 96873 | 8070.74 | 5.74 | 10.60 | 14.05 |
| redis | 100 | 107282 | 8935.22 | 10.54 | 17.95 | 23.36 |
| redis | 200 | 142155 | 11835.52 | 15.80 | 27.79 | 35.72 |
| sharded | 50 | 120689 | 10055.37 | 4.65 | 8.27 | 10.93 |
| sharded | 100 | 126507 | 10537.64 | 8.95 | 15.43 | 20.48 |
| sharded | 200 | 118338 | 9846.75 | 18.82 | 34.16 | 44.89 |

JSON：`db-vu20.json`、`db-vu50.json`、`redis-vu50.json`、`redis-vu100.json`、`redis-vu200.json`、`sharded-vu50.json`、`sharded-vu100.json`、`sharded-vu200.json`。

对照只限这张表：

- 50 VU 时，`redis` 的 QPS 是 `db` 的 8070.74 / 346.09 ≈ 23.3 倍，`sharded` 是 10055.37 / 346.09 ≈ 29.1 倍。P50 从 142.71 ms 降到 5.74 ms 和 4.65 ms。
- `db` 从 20 VU 加到 50 VU，QPS 没有上去（356.19 → 346.09），P50 从 55.28 ms 涨到 142.71 ms。请求在同一行库存上排队，加并发只是把锁等待拉长。
- 单机 Redis 上，分桶不是单调更快。50 VU 和 100 VU 时 `sharded` 高于单 key（10055.37 vs 8070.74，10537.64 vs 8935.22）。200 VU 时单 key 更高（11835.52 vs 9846.75，大约 1.20 倍）。Redis 执行命令是单线程的，16 个 key 还在同一个实例里，Lua 还要多访问几个 key，再平衡也会搬库存。分桶是为了多节点拆热 key，不是为了在这一台 Redis 上堆 QPS。

这三张券开测时订单表不是空的。`db` 路径的绝对 QPS 会受这个影响。对照关系来自同一次矩阵，不要和空表上的另一次运行混在一起。

## 消化完之后的账

容量对照结束、消费者把各自的成功请求写完之后，`cmd/reconcile` 的结果（`reconcile-final-18.json`、`reconcile-final-19.json`、`reconcile-final-20.json`）：

| 券 | 等式 | 超卖 | consistent |
|---|---|---:|---|
| 18 `db` | 有效订单 8489 + DB 剩余 291511 = 300000 | 0 | true |
| 19 `redis` | Redis 剩余 2653690 + 有效订单 346310 = 3000000，Redis 用户集合 346310 | 0 | true |
| 20 `sharded` | Redis 剩余 2634466 + 有效订单 365534 = 3000000，Redis 用户集合 365534 | 0 | true |

k6 成功笔数对得上：4297+4192=8489，96873+107282+142155=346310，120689+126507+118338=365534。

券 19 的那份快照里，共享流还有 `MQPending=75`、`MQLag=129681`。等式已经成立，这 346310 笔不在在途里，lag 是别的券还堆在同一条 `tb:stream:orders` 上。`Evaluate` 把共享流 lag 写进 notes，不因此把这张券判成不一致。券 20 的快照里 lag 是 0。

## 零超卖证明

券 17，`sharded`，8 个桶，库存 400。k6：80 VU，8 秒，`USER_BASE=900000000`。文件 `oversell-proof.json`、`oversell-proof-reconcile.json`。

| 项 | 值 |
|---|---|
| 请求数 | 88541 |
| QPS | 11059.57 |
| P50 / P95 / P99 | 6.50 / 13.70 / 18.47 ms |
| `grab_success` | 400 |
| `grab_sold_out` | 88141 |
| 5xx | 0（checks 88541 通过、0 失败） |

400+88141=88541。对账：Redis 剩余 0，有效订单 400，用户集合 400，负数桶 0，死信 0，`oversell=0`，`consistent=true`。成功笔数等于库存，多出来的请求全部是 409 `sold_out`。

另一次对账（`reconcile-final-17.json`）等式仍然是 0+400=400，`oversell=0`，但共享流上还有别的券的 `MQLag=173873`。这和“这张券卖了 400 张”不矛盾。

## 故障：8000 库存时停消费者

券 21，库存 8000，8 桶。`scripts/fault.sh`：60 VU、24 秒，第 6 秒 `docker compose stop consumer`。日志 `fault-consumer.log`，k6 是 `fault-consumer-k6.json`。

| 项 | 值 |
|---|---|
| 停消费者 | 07:44:17Z |
| 中途对账 | 07:44:25Z，已经 `consistent=true` |
| 请求数 / QPS | 366494 / 15268.67 |
| P50 / P95 / P99 | 3.47 / 7.38 / 10.41 ms |
| 成功 / 售罄 | 8000 / 358494 |
| 5xx | 0 |

中途和结束（`fault-consumer-mid.json`、`fault-consumer-final.json`）都是：Redis 剩余 0，有效订单 8000，用户集合 8000，lag 0，`oversell=0`。库存在停掉之前就卖完了，消费者也已经把这 8000 条写进 MySQL。这一轮证明了“售罄后停消费者不会把账打坏”，没有证明“停掉之后积压再恢复”。恢复实验是下一节。这一轮的 QPS 里绝大部分是售罄快速失败，不能拿去和容量表比。

## 故障：大库存时停消费者

券 24，库存 500000，8 桶。`scripts/fault-backlog.sh`：80 VU、18 秒，启动后 3 秒停消费者，k6 结束前不拉起。日志 `fault-backlog.log`。

k6（`fault-backlog-k6.json`）：请求数 246814，QPS 13709.77，P50 / P95 / P99 = 4.93 / 11.89 / 16.59 ms，全部 HTTP 200，`grab_success=246814`，5xx 为 0。消费者停着的时候 API 仍在扣 Redis。

停掉时刻 `2026-09-28T07:49:31Z`。两张快照：

| | 刚停下（`fault-backlog-mid.json`，07:49:32Z） | k6 结束、消费者仍停着（`fault-backlog-held.json`，07:49:46Z） |
|---|---|---|
| consistent | false | false |
| oversell | 0 | 0 |
| DB 有效订单 | 4352 | 4352 |
| Redis 用户集合 | 23166 | 246814 |
| Redis 剩余 | 476836 | 253186 |
| 共享流 lag | 18735 | 242379 |

停着的那十几秒里，DB 有效订单钉在 4352，Redis 用户集合从 23166 涨到 246814，和 k6 的成功笔数相同。结束时：

- 用户集合 − 已落库 = 246814 − 4352 = 242462
- 总库存 −（Redis 剩余 + 已落库）= 500000 − (253186 + 4352) = 242462

两边相等，差额是还在流里的预扣，不是超卖。刚停下的那一张快照差 2（18814 和 18812）：当时 k6 还在打，对账读 Redis 剩余和用户集合不是同一个瞬间。安静之后的 held 快照没有这个差。

`2026-09-28T07:49:47Z` 拉起消费者，`07:50:45Z` 对账退出码 0（`fault-backlog-final.json`）：有效订单 246814，Redis 剩余 253186，用户集合 246814，lag 0，死信 0，`oversell=0`，`consistent=true`。253186+246814=500000。从拉起到账平大约 58 秒。

## 故障：重启 Redis

券 22，库存 8000，8 桶。40 VU、24 秒，第 6 秒 `docker compose restart redis`。日志 `fault-redis.log`。

| 项 | 值 |
|---|---|
| 开始重启 | 07:44:43Z |
| `PING` 恢复 | 07:45:10Z（约 27 秒） |
| 请求数 / QPS | 81025 / 3360.50 |
| 全部请求 P50 / P95 / P99 / max | 2.76 / 8.44 / 180.94 / 7192.13 ms |
| 仅 HTTP 200 的 P50 / P95 / P99 / max | 4.26 / 7.19 / 9.38 / 15.47 ms |
| 成功 / 售罄 / `grab_failed` | 8000 / 69710 / 3315 |
| “not 5xx” | 77710 / 81025 = 95.91% |

8000+69710+3315=81025。失败的 3315 笔是 Redis 不可达时的 HTTP 500。整体 P99 被这 27 秒的超时拉到 180.94 ms，最长 7.19 秒；成功请求自己的 P99 仍是 9.38 ms。

恢复后的对账（`fault-redis-final.json`）：Redis 剩余 0，有效订单 8000，用户集合 8000，`oversell=0`，`consistent=true`。AOF 把重启前的预扣留住了，没有多卖。这一轮是 `docker compose restart`，进程正常退出再起来，不是 `kill -9`，所以没有测到 `appendfsync everysec` 丢掉最后一秒的那种少卖。

## 复现

```bash
docker compose up -d --build
bash scripts/bench.sh           # 新券，不会覆盖券 18/19/20 的历史 JSON 名字，会覆盖同名 summary 文件
bash scripts/fault.sh           # 券名 fault-consumer / fault-redis，库存 8000
bash scripts/fault-backlog.sh   # 库存 500000，停到 k6 结束
```

`bench.sh` 会覆盖 `docs/bench-results/` 里同名的 k6 JSON。上面的表对应 2026-09-28 落盘的文件。再跑一次会得到新券和新数字，以新 JSON 为准，不要把两次的 QPS 抄进同一张表。

## 这组实验没有覆盖的

- 没有把 16 个桶放到多台 Redis 上再压。单机结果不能写成“分桶提升了 QPS”。
- 没有用 `kill -9` 打 Redis 去看 AOF 丢一秒。
- 容量数字是在全局限流关闭、每用户只请求一次的条件下测的。
- 8000 库存的停消费者实验没有积压；积压和恢复只在券 24 上测到。

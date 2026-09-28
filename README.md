# 茶歇抢券 / Teabreak

模块路径：[`github.com/victorzhong0110/teabreak`](https://github.com/victorzhong0110/teabreak)

连锁茶饮品牌整点抢券（例如下午 3 点放 1 万张半价券）：不超发、一人一张、未支付订单超时自动回补库存。

实现是 Redis Lua 预扣库存、热点库存分桶、一人一单、幂等、限流、Redis Streams 异步落单、超时还库存、对账。设计说明在 [`docs/design.md`](docs/design.md)，均匀压测在 [`docs/benchmark.md`](docs/benchmark.md)，突发和多实例在 [`docs/benchmark-realistic.md`](docs/benchmark-realistic.md)。

文档里的 QPS、延迟和笔数都来自仓库里的 k6 结果，没有另写无法复现的数字。

## 跑起来

依赖：Docker、Docker Compose、Go 1.22（只跑测试或对账时才需要本机 Go）。

```bash
docker compose up -d --build
curl -s http://127.0.0.1:18080/healthz
```

端口（故意避开 3306 / 6379 / 8080）：

| 服务 | 地址 |
|---|---|
| API | http://127.0.0.1:18080 |
| 消费者指标 | http://127.0.0.1:18081/metrics |
| Prometheus | http://127.0.0.1:19090 |
| pprof | http://127.0.0.1:16060/debug/pprof/ |
| MySQL | `127.0.0.1:13306`，账号 `root` / `root`，库 `coupon` |
| Redis | `127.0.0.1:16379` |

创建一张 100 张的分桶券并抢一次：

```bash
curl -s -X POST http://127.0.0.1:18080/v1/coupons \
  -H 'Content-Type: application/json' \
  -d '{"name":"开学季奶茶券","total_stock":100,"shard_count":8,"strategy":"sharded","pay_timeout_sec":900}'

# 把下面的 1 换成返回的 id
curl -s -X POST http://127.0.0.1:18080/v1/coupons/1/grab \
  -H 'X-User-Id: 42' -H 'Idempotency-Key: demo-42'

curl -s http://127.0.0.1:18080/v1/coupons/1/stock
curl -s http://127.0.0.1:18080/v1/coupons/1/reconcile
```

同一个 `Idempotency-Key` 再请求一次，返回 `replay: true`，库存不变。换一个键、同一个用户，返回 `already_owned`。

`X-User-Id` 是演示用的身份，没有登录校验。生产环境不能这样。

## 三种扣库存方式

创建券时选 `strategy`，HTTP 和订单表是同一套，方便对比：

| strategy | 含义 |
|---|---|
| `db` | 请求里用事务扣 `coupons.remaining` 并插入订单。慢基线 |
| `redis` | 单个 Redis key + Lua，订单异步落库 |
| `sharded` | 多个 Redis key + 同一条 Lua，外加再平衡 |

`coupons.remaining` 只对 `db` 有意义。Redis 路径看 `/v1/coupons/:id/stock`。

全局限流默认关闭（`GLOBAL_RPS=0`），否则压测打到的是限流器。打开的方式：

```bash
GLOBAL_RPS=1000 USER_LIMIT=30 docker compose up -d api
```

用户维度滑动窗口默认每个用户 10 秒 30 次。

## 测试

```bash
# 不依赖数据库的单元测试，含 Lua（miniredis）和 -race 能覆盖的令牌桶
go test -race -count=1 ./internal/...

# 集成测试：先把 MySQL 和 Redis 拉起来
docker compose up -d mysql redis
export MYSQL_DSN='root:root@tcp(127.0.0.1:13306)/coupon?parseTime=true&charset=utf8mb4&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s'
export REDIS_ADDR='127.0.0.1:16379'
go test -race -count=1 ./internal/integration/ ./...
```

集成测试覆盖：分桶不超卖、一人一单和幂等回放、超时还库存、DB 直写不超卖、用户限流、HTTP 对账、毒消息进死信。没设环境变量时这些测试会跳过，单元测试仍然通过。GitHub Actions 会起 MySQL 和 Redis 跑全部测试。

## 压测和对账

```bash
bash scripts/bench.sh          # 三种策略的 k6，以及小库存超卖证明
bash scripts/fault.sh          # 小库存下停消费者、重启 Redis
bash scripts/fault-backlog.sh  # 大库存下停消费者，直到 k6 结束再拉起，看积压后账平
bash scripts/bench-realistic.sh # 突发、Zipf、多实例、资源上限、故障恢复
```

原始 JSON 在 `docs/bench-results/`。`cmd/reconcile` 退出码 0 表示账平：

```bash
export MYSQL_DSN='root:root@tcp(127.0.0.1:13306)/coupon?parseTime=true&charset=utf8mb4&loc=UTC'
export REDIS_ADDR='127.0.0.1:16379'
go run ./cmd/reconcile -coupon 1 -wait 30s
```

## 目录

```
cmd/api            HTTP
cmd/consumer       消费、超时取消、再平衡
cmd/reconcile      对账
internal/stock     Lua、分桶、再平衡
internal/seckill   抢券规则
internal/worker    Redis Streams 消费者
internal/fulfill   取消并还库存
internal/cache     布隆过滤器、singleflight、逻辑过期
internal/order     MySQL
internal/reconcile 一致性公式
```

## English

A Go service for a tea-chain hourly coupon drop (for example, 10,000 half-price coupons at 3pm): no oversell, one coupon per person, and unpaid orders return stock when they time out.

The API pre-deducts stock with a single Redis Lua script (idempotency key, one-coupon-per-user, sharded counters, and a Redis Stream append). A separate consumer inserts MySQL rows, retries via `XAUTOCLAIM`, and parks poison messages on a dead-letter stream. Expired unpaid orders return stock exactly once. A reconciler checks that Redis stock plus live orders equals the original supply.

```bash
docker compose up -d --build
curl -s http://127.0.0.1:18080/healthz
```

Strategies on `POST /v1/coupons`: `db` (row lock baseline), `redis` (one key), `sharded` (many keys, same Lua). Load tests and fault injection:

```bash
bash scripts/bench.sh
bash scripts/fault.sh
bash scripts/fault-backlog.sh
bash scripts/bench-realistic.sh
```

Measured numbers, machine specs, and the oversell check are in `docs/benchmark.md`. The bursty, multi-instance run is in `docs/benchmark-realistic.md`. Design notes (Chinese) are in `docs/design.md`.

Redis Streams is the queue because the stock decrement and the enqueue must commit together. Kafka cannot share a transaction with Redis; using it on the hot path would open a window where stock is taken and the message is not. Trade-offs are documented in the design note.

## References / 致谢

实现是原创的，没有复制下面仓库的代码。无许可证的仓库只读了设计，没有拷文件。

| 仓库 | 许可证 | 借鉴了什么 |
|---|---|---|
| [qiurunze123/miaosha](https://github.com/qiurunze123/miaosha) | 未声明，只读文档 | 秒杀链路的拆法：页面之外的 Redis 预减、消息队列异步下单、接口防刷。本项目做成抢券，并补上分桶和对账 |
| [CocaineCong/Go-SecKill](https://github.com/CocaineCong/Go-SecKill) | MIT | 把互斥锁、乐观锁、Redis、etcd 锁放在一起对比的写法。本项目的结论和代码是自己的：热路径用 Lua，不用分布式锁；DB 行锁只留作压测基线 |
| [java-up-up/hmdp-plus](https://github.com/java-up-up/hmdp-plus) | Apache-2.0 | “黑马点评怎么做深”：限流、消息可靠性、一致性要形成闭环。本项目用对账程序和故障注入把闭环跑通，而不是停在接口能通 |
| [zhongzhh8/SecKill-System](https://github.com/zhongzhh8/SecKill-System) | 未声明，只读思路 | Gin + Redis Lua 可以做最小扣减。本项目的脚本同时处理分桶、一人一单和入队，没有照搬那个仓库的脚本 |

其他对照过、但没有拿来改的：[alibaba/Sentinel](https://github.com/alibaba/Sentinel)（限流算法）、[grafana/k6](https://github.com/grafana/k6)（压测工具，AGPL-3.0，只作为独立二进制使用）。

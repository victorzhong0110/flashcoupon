// Package stock 管 Redis 里的库存。
//
// 热路径只有一条 Lua：幂等键、一人一单、分桶扣减、XADD 订单流在同一次脚本执行里完成。
// Redis 跑 Lua 时不会插入别的命令，所以这里不需要 SETNX 分布式锁。
// 锁能互斥，但锁本身会成为热点，而且忘了续期或删错锁都会出事故。
package stock

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrSoldOut     = errors.New("sold_out")
	ErrAlready     = errors.New("already_owned")
	ErrNoShard     = errors.New("no_shard")
	ErrRateLimited = errors.New("rate_limited")
)

// grabLua 的返回值第一位是状态码：
//
//	1 成功，后面跟着 payload、分桶下标、该桶剩余
//	2 同一个幂等键已经成功过，第二位是当时的 payload
//	3 这个用户已经占着库存（换了一个幂等键再来）
//	4 没有分桶（调用方 bug）
//	5 所有桶都是 0
//
// 先判断库存再 DECR，而不是 DECR 之后发现变成负数再加回去。
// “减完再判断”在没有 Lua 的时候会和另一个客户端交错，出现短暂超卖窗口。
// 脚本里即使先减再加也安全，但写成先判断，读代码的人不会误以为外面也能这么写。
const grabLua = `
local existing = redis.call('GET', KEYS[1])
if existing then
  return {2, existing}
end
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
  return {3, ''}
end
local n = #KEYS - 3
if n <= 0 then
  return {4, ''}
end
local prefer = tonumber(ARGV[5]) % n
local chosen = -1
local left = -1
for i = 0, n - 1 do
  local idx = (prefer + i) % n
  local key = KEYS[4 + idx]
  local stock = tonumber(redis.call('GET', key) or '0')
  if stock > 0 then
    left = redis.call('DECR', key)
    chosen = idx
    break
  end
end
if chosen < 0 then
  return {5, ''}
end
redis.call('SADD', KEYS[2], ARGV[1])
local payload = ARGV[4] .. '|' .. ARGV[1] .. '|' .. ARGV[2] .. '|' .. ARGV[3] .. '|' .. chosen .. '|' .. ARGV[7] .. '|' .. ARGV[8]
redis.call('SET', KEYS[1], payload, 'EX', tonumber(ARGV[6]))
redis.call('XADD', KEYS[3], '*',
  'order_id', ARGV[4],
  'user_id', ARGV[1],
  'coupon_id', ARGV[2],
  'idem', ARGV[3],
  'shard', tostring(chosen),
  'expire_at_ms', ARGV[7],
  'created_at_ms', ARGV[8])
return {1, payload, chosen, left}
`

// grabCombinedLua 把「幂等回放、用户滑动窗口、一人一单、扣库存、入队」合成一次往返。
// 拆开做的时候，新用户要先 GET 幂等键、再跑限流脚本、再跑扣库存脚本，Redis 单线程会被往返次数放大。
// 回放必须放在扣令牌之前：客户端超时重试不该把自己打进 429，否则用户会以为没抢到。
// 返回码 6 表示滑动窗口已满，库存和用户集合都没动。
const grabCombinedLua = `
local existing = redis.call('GET', KEYS[1])
if existing then
  return {2, existing}
end
local grps = tonumber(ARGV[14])
if grps > 0 then
  local cap = tonumber(ARGV[15])
  local nowg = tonumber(ARGV[10])
  local data = redis.call('GET', KEYS[5])
  local tokens = cap
  local last = nowg
  if data then
    local sep = string.find(data, '|', 1, true)
    tokens = tonumber(string.sub(data, 1, sep - 1))
    last = tonumber(string.sub(data, sep + 1))
    local delta = math.max(0, nowg - last) / 1000.0
    tokens = math.min(cap, tokens + delta * grps)
  end
  if tokens < 1 then
    redis.call('SET', KEYS[5], tokens .. '|' .. nowg, 'PX', 10000)
    return {6, 'global'}
  end
  tokens = tokens - 1
  redis.call('SET', KEYS[5], tokens .. '|' .. nowg, 'PX', 10000)
end
local limit = tonumber(ARGV[9])
if limit > 0 then
  redis.call('ZREMRANGEBYSCORE', KEYS[4], '0', ARGV[11])
  local n = redis.call('ZCARD', KEYS[4])
  if n >= limit then
    return {6, tostring(n)}
  end
  redis.call('ZADD', KEYS[4], ARGV[10], ARGV[12])
  redis.call('PEXPIRE', KEYS[4], ARGV[13])
end
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
  return {3, ''}
end
local nshard = #KEYS - 5
if nshard <= 0 then
  return {4, ''}
end
local prefer = tonumber(ARGV[5]) % nshard
local chosen = -1
local left = -1
for i = 0, nshard - 1 do
  local idx = (prefer + i) % nshard
  local key = KEYS[6 + idx]
  local stock = tonumber(redis.call('GET', key) or '0')
  if stock > 0 then
    left = redis.call('DECR', key)
    chosen = idx
    break
  end
end
if chosen < 0 then
  return {5, ''}
end
redis.call('SADD', KEYS[2], ARGV[1])
local payload = ARGV[4] .. '|' .. ARGV[1] .. '|' .. ARGV[2] .. '|' .. ARGV[3] .. '|' .. chosen .. '|' .. ARGV[7] .. '|' .. ARGV[8]
redis.call('SET', KEYS[1], payload, 'EX', tonumber(ARGV[6]))
redis.call('XADD', KEYS[3], '*',
  'order_id', ARGV[4],
  'user_id', ARGV[1],
  'coupon_id', ARGV[2],
  'idem', ARGV[3],
  'shard', tostring(chosen),
  'expire_at_ms', ARGV[7],
  'created_at_ms', ARGV[8])
return {1, payload, chosen, left}
`

// returnLua 用 SREM 的返回值当“只还一次”的开关。
// 超时任务重试时，用户已经不在集合里，就绝不能再 INCR。
const returnLua = `
local removed = redis.call('SREM', KEYS[1], ARGV[1])
if removed == 1 then
  local left = redis.call('INCRBY', KEYS[2], tonumber(ARGV[2]))
  return {1, left}
end
return {0, tonumber(redis.call('GET', KEYS[2]) or '0')}
`

// moveLua 搬运时再次检查源桶够不够。
// 计划和执行之间用户还在抢，源桶可能已经空了，这时必须放弃而不是扣成负数。
const moveLua = `
local have = tonumber(redis.call('GET', KEYS[1]) or '0')
local n = tonumber(ARGV[1])
if n <= 0 or have < n then
  return {0, have, tonumber(redis.call('GET', KEYS[2]) or '0')}
end
local src = redis.call('DECRBY', KEYS[1], n)
local dst = redis.call('INCRBY', KEYS[2], n)
return {1, src, dst}
`

// initLua 用 warmed 标记保证重启不会把库存覆盖回初始值。
// 标记和各桶 SET 在同一个脚本里，不会出现“标记写上了但某个桶还是空的”。
const initLua = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
for i = 2, #KEYS do
  redis.call('SET', KEYS[i], ARGV[i - 1])
end
redis.call('SET', KEYS[1], '1')
return 1
`

type GrabParams struct {
	CouponID  int64
	UserID    int64
	IdemKey   string
	OrderID   int64
	ShardN    int
	ExpireAt  time.Time
	CreatedAt time.Time
}

type GrabOutcome struct {
	OrderID int64
	UserID  int64
	Shard   int
	Left    int
	Replay  bool
	// Raw 是幂等键里存的 payload，回放时用它还原订单号。
	Raw string
}

type RedisStock struct {
	rdb      redis.UniversalClient
	stream   string
	idemTTL  time.Duration
	grab     *redis.Script
	combined *redis.Script
	ret      *redis.Script
	move     *redis.Script
	init     *redis.Script
	seq      atomic.Uint64
}

func New(rdb redis.UniversalClient, stream string, idemTTL time.Duration) *RedisStock {
	if idemTTL <= 0 {
		idemTTL = 24 * time.Hour
	}
	return &RedisStock{
		rdb:      rdb,
		stream:   stream,
		idemTTL:  idemTTL,
		grab:     redis.NewScript(grabLua),
		combined: redis.NewScript(grabCombinedLua),
		ret:      redis.NewScript(returnLua),
		move:     redis.NewScript(moveLua),
		init:     redis.NewScript(initLua),
	}
}

func (s *RedisStock) Stream() string { return s.stream }

func StockKey(couponID int64, shard int) string {
	return fmt.Sprintf("tb:coupon:%d:stock:%d", couponID, shard)
}

func UsersKey(couponID int64) string {
	return fmt.Sprintf("tb:coupon:%d:users", couponID)
}

func IdemKey(couponID int64, idem string) string {
	return fmt.Sprintf("tb:coupon:%d:idem:%s", couponID, idem)
}

func WarmedKey(couponID int64) string {
	return fmt.Sprintf("tb:coupon:%d:warmed", couponID)
}

func MetaKey(couponID int64) string {
	return fmt.Sprintf("tb:coupon:%d:meta", couponID)
}

// Init 在 warmed 标记不存在时写入各桶。已经初始化过则什么都不改，返回 false。
func (s *RedisStock) Init(ctx context.Context, couponID int64, levels []int) (bool, error) {
	if len(levels) == 0 {
		return false, ErrNoShard
	}
	keys := make([]string, 0, len(levels)+1)
	args := make([]interface{}, 0, len(levels))
	keys = append(keys, WarmedKey(couponID))
	for i, n := range levels {
		keys = append(keys, StockKey(couponID, i))
		args = append(args, n)
	}
	v, err := s.init.Run(ctx, s.rdb, keys, args...).Result()
	if err != nil {
		return false, err
	}
	return toInt(v) == 1, nil
}

func (s *RedisStock) Grab(ctx context.Context, p GrabParams) (GrabOutcome, error) {
	if p.ShardN < 1 {
		return GrabOutcome{}, ErrNoShard
	}
	keys := make([]string, 0, 3+p.ShardN)
	keys = append(keys,
		IdemKey(p.CouponID, p.IdemKey),
		UsersKey(p.CouponID),
		s.stream,
	)
	for i := 0; i < p.ShardN; i++ {
		keys = append(keys, StockKey(p.CouponID, i))
	}
	prefer := PreferShard(p.UserID, p.ShardN)
	args := []interface{}{
		strconv.FormatInt(p.UserID, 10),
		strconv.FormatInt(p.CouponID, 10),
		p.IdemKey,
		strconv.FormatInt(p.OrderID, 10),
		prefer,
		int(s.idemTTL.Seconds()),
		strconv.FormatInt(p.ExpireAt.UnixMilli(), 10),
		strconv.FormatInt(p.CreatedAt.UnixMilli(), 10),
	}
	v, err := s.grab.Run(ctx, s.rdb, keys, args...).Result()
	if err != nil {
		return GrabOutcome{}, err
	}
	return parseGrab(v)
}

// UserRateKey 和 ratelimit.AllowUser 用同一套 key，方便对照两种热路径。
func UserRateKey(couponID, userID int64) string {
	return fmt.Sprintf("tb:rl:user:%d:%d", couponID, userID)
}

// GrabCombined 用一条 Lua 完成回放、全局限流、用户限流和扣库存。
// 幂等回放在扣令牌之前返回。limit<=0 或 globalRPS<=0 时跳过对应的限流，库存逻辑不变。
func (s *RedisStock) GrabCombined(ctx context.Context, p GrabParams, limit int, window time.Duration, globalRPS float64, globalBurst int) (GrabOutcome, error) {
	if p.ShardN < 1 {
		return GrabOutcome{}, ErrNoShard
	}
	if window <= 0 {
		window = 10 * time.Second
	}
	if globalBurst < 1 {
		globalBurst = 1
	}
	now := time.Now().UnixMilli()
	cutoff := now - window.Milliseconds()
	member := strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.FormatUint(s.seq.Add(1), 10)
	keys := make([]string, 0, 5+p.ShardN)
	keys = append(keys,
		IdemKey(p.CouponID, p.IdemKey),
		UsersKey(p.CouponID),
		s.stream,
		UserRateKey(p.CouponID, p.UserID),
		"tb:rl:global",
	)
	for i := 0; i < p.ShardN; i++ {
		keys = append(keys, StockKey(p.CouponID, i))
	}
	args := []interface{}{
		strconv.FormatInt(p.UserID, 10),
		strconv.FormatInt(p.CouponID, 10),
		p.IdemKey,
		strconv.FormatInt(p.OrderID, 10),
		PreferShard(p.UserID, p.ShardN),
		int(s.idemTTL.Seconds()),
		strconv.FormatInt(p.ExpireAt.UnixMilli(), 10),
		strconv.FormatInt(p.CreatedAt.UnixMilli(), 10),
		limit,
		now,
		cutoff,
		member,
		window.Milliseconds(),
		globalRPS,
		globalBurst,
	}
	v, err := s.combined.Run(ctx, s.rdb, keys, args...).Result()
	if err != nil {
		return GrabOutcome{}, err
	}
	return parseGrab(v)
}

// Return 把 1 个库存还回当初扣的那个桶，并把用户从持有集合里去掉。
// 返回 true 表示这次确实还了库存；false 表示之前已经还过。
func (s *RedisStock) Return(ctx context.Context, couponID, userID int64, shard int) (bool, error) {
	v, err := s.ret.Run(ctx, s.rdb,
		[]string{UsersKey(couponID), StockKey(couponID, shard)},
		strconv.FormatInt(userID, 10), 1,
	).Result()
	if err != nil {
		return false, err
	}
	arr, ok := v.([]interface{})
	if !ok || len(arr) < 1 {
		return false, fmt.Errorf("bad return lua reply %v", v)
	}
	return toInt(arr[0]) == 1, nil
}

// Move 在两个桶之间搬 amount。源桶不够时返回 false，不改任何 key。
func (s *RedisStock) Move(ctx context.Context, couponID int64, src, dst, amount int) (bool, error) {
	v, err := s.move.Run(ctx, s.rdb,
		[]string{StockKey(couponID, src), StockKey(couponID, dst)},
		amount,
	).Result()
	if err != nil {
		return false, err
	}
	arr, ok := v.([]interface{})
	if !ok || len(arr) < 1 {
		return false, fmt.Errorf("bad move lua reply %v", v)
	}
	return toInt(arr[0]) == 1, nil
}

func (s *RedisStock) Levels(ctx context.Context, couponID int64, n int) ([]int, error) {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = StockKey(couponID, i)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]int, n)
	for i, v := range vals {
		if v == nil {
			out[i] = 0
			continue
		}
		out[i] = toInt(v)
	}
	return out, nil
}

func (s *RedisStock) UserCount(ctx context.Context, couponID int64) (int64, error) {
	return s.rdb.SCard(ctx, UsersKey(couponID)).Result()
}

func (s *RedisStock) AddUsers(ctx context.Context, couponID int64, userIDs []int64) error {
	if len(userIDs) == 0 {
		return nil
	}
	members := make([]interface{}, len(userIDs))
	for i, id := range userIDs {
		members[i] = strconv.FormatInt(id, 10)
	}
	return s.rdb.SAdd(ctx, UsersKey(couponID), members...).Err()
}

// GetIdem 读幂等键。没有这条记录时返回空串，不是错误。
func (s *RedisStock) GetIdem(ctx context.Context, couponID int64, idem string) (string, error) {
	v, err := s.rdb.Get(ctx, IdemKey(couponID, idem)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

func (s *RedisStock) Warmed(ctx context.Context, couponID int64) (bool, error) {
	n, err := s.rdb.Exists(ctx, WarmedKey(couponID)).Result()
	return n == 1, err
}

func parseGrab(v interface{}) (GrabOutcome, error) {
	arr, ok := v.([]interface{})
	if !ok || len(arr) < 2 {
		return GrabOutcome{}, fmt.Errorf("bad grab lua reply %T %v", v, v)
	}
	switch toInt(arr[0]) {
	case 1:
		raw := toString(arr[1])
		out, err := parsePayload(raw)
		if err != nil {
			return GrabOutcome{}, err
		}
		out.Raw = raw
		if len(arr) > 3 {
			out.Left = toInt(arr[3])
		}
		return out, nil
	case 2:
		raw := toString(arr[1])
		out, err := parsePayload(raw)
		if err != nil {
			return GrabOutcome{}, err
		}
		out.Replay = true
		out.Raw = raw
		return out, nil
	case 3:
		return GrabOutcome{}, ErrAlready
	case 4:
		return GrabOutcome{}, ErrNoShard
	case 5:
		return GrabOutcome{}, ErrSoldOut
	case 6:
		return GrabOutcome{}, ErrRateLimited
	default:
		return GrabOutcome{}, fmt.Errorf("unknown grab code %v", arr[0])
	}
}

// ParsePayload 解析 Lua 写在幂等键里的字符串，HTTP 回放和测试都用它。
func ParsePayload(raw string) (GrabOutcome, error) {
	return parsePayload(raw)
}

func parsePayload(raw string) (GrabOutcome, error) {
	parts := strings.Split(raw, "|")
	if len(parts) != 7 {
		return GrabOutcome{}, fmt.Errorf("bad payload %q", raw)
	}
	orderID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return GrabOutcome{}, err
	}
	userID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return GrabOutcome{}, err
	}
	shard, err := strconv.Atoi(parts[4])
	if err != nil {
		return GrabOutcome{}, err
	}
	return GrabOutcome{OrderID: orderID, UserID: userID, Shard: shard}, nil
}

func toInt(v interface{}) int {
	switch t := v.(type) {
	case int64:
		return int(t)
	case int:
		return t
	case string:
		n, _ := strconv.Atoi(t)
		return n
	case []byte:
		n, _ := strconv.Atoi(string(t))
		return n
	default:
		return 0
	}
}

func toString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

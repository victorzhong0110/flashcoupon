package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// 滑动窗口用 ZSET：score 是毫秒时间戳，member 必须唯一。
// 如果 member 用“当前毫秒”，同一毫秒的多次请求会互相覆盖，限流会偏松。
// 这里用纳秒加一个进程内序号。
const windowLua = `
redis.call('ZREMRANGEBYSCORE', KEYS[1], '0', ARGV[1])
local n = redis.call('ZCARD', KEYS[1])
if n >= tonumber(ARGV[3]) then
  return {0, n}
end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return {1, n + 1}
`

// 令牌桶把“当前令牌数|上次补充的毫秒”放在一个字符串里。
// 脚本执行期间没有别的命令能插入，所以不用再加锁。
const bucketLua = `
local cap = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local need = tonumber(ARGV[4])
local data = redis.call('GET', KEYS[1])
local tokens = cap
local last = now
if data then
  local sep = string.find(data, '|', 1, true)
  tokens = tonumber(string.sub(data, 1, sep - 1))
  last = tonumber(string.sub(data, sep + 1))
  local delta = math.max(0, now - last) / 1000.0
  tokens = math.min(cap, tokens + delta * rate)
end
local allowed = 0
if tokens >= need then
  tokens = tokens - need
  allowed = 1
end
redis.call('SET', KEYS[1], tokens .. '|' .. now, 'PX', ARGV[5])
return {allowed, tokens}
`

type RedisLimiter struct {
	rdb    redis.UniversalClient
	window *redis.Script
	bucket *redis.Script
	seq    atomic.Uint64
}

func NewRedis(rdb redis.UniversalClient) *RedisLimiter {
	return &RedisLimiter{
		rdb:    rdb,
		window: redis.NewScript(windowLua),
		bucket: redis.NewScript(bucketLua),
	}
}

// AllowUser 限制某个用户对某张券的请求频率。limit<=0 时关闭。
func (l *RedisLimiter) AllowUser(ctx context.Context, couponID, userID int64, limit int, window time.Duration) (bool, error) {
	if limit <= 0 {
		return true, nil
	}
	now := time.Now().UnixMilli()
	cutoff := now - window.Milliseconds()
	member := strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.FormatUint(l.seq.Add(1), 10)
	key := fmt.Sprintf("tb:rl:user:%d:%d", couponID, userID)
	v, err := l.window.Run(ctx, l.rdb, []string{key},
		cutoff, now, limit, member, window.Milliseconds(),
	).Result()
	if err != nil {
		return false, err
	}
	return codeOK(v), nil
}

// AllowGlobal 是全集群共用的一把令牌桶。rps<=0 时关闭。
func (l *RedisLimiter) AllowGlobal(ctx context.Context, rps float64, burst int) (bool, error) {
	if rps <= 0 {
		return true, nil
	}
	if burst < 1 {
		burst = 1
	}
	// 桶本身 10 秒没人打就过期，避免冷 key 常驻；过期后会按满桶重建。
	v, err := l.bucket.Run(ctx, l.rdb, []string{"tb:rl:global"},
		burst, rps, time.Now().UnixMilli(), 1, 10_000,
	).Result()
	if err != nil {
		return false, err
	}
	return codeOK(v), nil
}

func codeOK(v interface{}) bool {
	arr, ok := v.([]interface{})
	if !ok || len(arr) == 0 {
		return false
	}
	switch n := arr[0].(type) {
	case int64:
		return n == 1
	case int:
		return n == 1
	default:
		return false
	}
}

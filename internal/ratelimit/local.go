// 进程内令牌桶，当作网关前面的廉价挡板。
// 多实例时每个进程各有一桶，总放行量约等于实例数乘以速率；
// 需要全集群精确限额时用 RedisTokenBucket。
package ratelimit

import (
	"sync"
	"time"
)

type LocalBucket struct {
	mu     sync.Mutex
	tokens float64
	burst  float64
	rate   float64 // 每秒补充多少个
	last   time.Time
}

func NewLocal(rate float64, burst int) *LocalBucket {
	if burst < 1 {
		burst = 1
	}
	return &LocalBucket{
		tokens: float64(burst),
		burst:  float64(burst),
		rate:   rate,
		last:   time.Now(),
	}
}

// Allow 取走 1 个令牌。rate<=0 时视为关闭，始终放行。
func (b *LocalBucket) Allow() bool {
	if b == nil || b.rate < 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	b.last = now
	if b.rate > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens -= 1
	return true
}

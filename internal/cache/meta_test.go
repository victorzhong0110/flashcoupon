package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/victorzhong0110/teabreak/internal/model"
)

func TestSingleflightAndBloom(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	var hits atomic.Int32
	m := NewMeta(rdb, func(ctx context.Context, id int64) (*model.Coupon, error) {
		hits.Add(1)
		time.Sleep(30 * time.Millisecond)
		if id != 42 {
			return nil, nil
		}
		return &model.Coupon{
			ID: 42, Name: "奶茶券", TotalStock: 10, ShardCount: 2,
			Strategy: model.StrategySharded, PerUserLimit: 1,
			StartAt: time.Now().Add(-time.Minute), EndAt: time.Now().Add(time.Hour),
			PayTimeoutSec: 60,
		}, nil
	}, Options{SoftTTL: time.Minute, HardTTL: 2 * time.Minute, NegTTL: time.Second, Jitter: 0})

	// 没进过布隆过滤器的 id 不能打到 Loader。
	if _, err := m.Get(context.Background(), 999); err != ErrNotFound {
		t.Fatalf("got %v", err)
	}
	if hits.Load() != 0 {
		t.Fatalf("penetration reached loader, hits=%d", hits.Load())
	}

	m.bloom.Add(42)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := m.Get(context.Background(), 42)
			if err != nil || c == nil || c.Name != "奶茶券" {
				t.Errorf("get %v %v", c, err)
			}
		}()
	}
	wg.Wait()
	if hits.Load() != 1 {
		t.Fatalf("singleflight hits=%d, want 1", hits.Load())
	}

	// 第二次走本地缓存，不再加载。
	if _, err := m.Get(context.Background(), 42); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Fatalf("cache miss, hits=%d", hits.Load())
	}
}

func TestBloomMissLoadsFromRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	var hits atomic.Int32
	m := NewMeta(rdb, func(ctx context.Context, id int64) (*model.Coupon, error) {
		hits.Add(1)
		return nil, nil
	}, Options{SoftTTL: time.Minute, HardTTL: 2 * time.Minute, NegTTL: time.Second, Jitter: 0})
	other := NewMeta(rdb, func(ctx context.Context, id int64) (*model.Coupon, error) {
		t.Fatal("other instance must not hit MySQL")
		return nil, nil
	}, Options{SoftTTL: time.Minute, HardTTL: 2 * time.Minute, NegTTL: time.Second, Jitter: 0})
	c := &model.Coupon{
		ID: 7, Name: "跨实例券", TotalStock: 3, ShardCount: 2,
		Strategy: model.StrategySharded, PerUserLimit: 1,
		StartAt: time.Now().Add(-time.Minute), EndAt: time.Now().Add(time.Hour),
		PayTimeoutSec: 60,
	}
	m.Remember(c)
	if err := m.PutRedis(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	got, err := other.Get(context.Background(), 7)
	if err != nil || got == nil || got.Name != "跨实例券" {
		t.Fatalf("other get %+v %v", got, err)
	}
	if hits.Load() != 0 {
		t.Fatalf("loader hits %d", hits.Load())
	}
}

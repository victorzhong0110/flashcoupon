package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestSlidingWindowUniqueMembers(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	lim := NewRedis(rdb)
	ctx := context.Background()
	allowed := 0
	for i := 0; i < 10; i++ {
		ok, err := lim.AllowUser(ctx, 1, 7, 3, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("allowed %d, want 3", allowed)
	}
}

func TestTokenBucketBurstThenDeny(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	lim := NewRedis(rdb)
	ctx := context.Background()
	allowed := 0
	for i := 0; i < 20; i++ {
		ok, err := lim.AllowGlobal(ctx, 0.001, 5)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("allowed %d, want burst 5 (rate is tiny so no refill during the loop)", allowed)
	}
}

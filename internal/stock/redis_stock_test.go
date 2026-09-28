package stock

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) (*RedisStock, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, "tb:stream:test", time.Hour), mr
}

func TestLuaNoOversellAndOneUser(t *testing.T) {
	st, _ := newTestRedis(t)
	ctx := context.Background()
	ok, err := st.Init(ctx, 1, Split(50, 4))
	if err != nil || !ok {
		t.Fatalf("init %v %v", ok, err)
	}
	again, err := st.Init(ctx, 1, Split(999, 4))
	if err != nil || again {
		t.Fatalf("second init must be a no-op, ok=%v err=%v", again, err)
	}

	var success atomic.Int64
	var soldOut atomic.Int64
	var already atomic.Int64
	var wg sync.WaitGroup
	// 200 个用户每人抢 2 次（不同幂等键），库存只有 50。
	for u := int64(1); u <= 200; u++ {
		for try := 0; try < 2; try++ {
			wg.Add(1)
			go func(user int64, try int) {
				defer wg.Done()
				_, err := st.Grab(ctx, GrabParams{
					CouponID:  1,
					UserID:    user,
					IdemKey:   "u" + itoa(user) + "-" + itoa(int64(try)),
					OrderID:   user*10 + int64(try),
					ShardN:    4,
					ExpireAt:  time.Now().Add(time.Minute),
					CreatedAt: time.Now(),
				})
				switch {
				case err == nil:
					success.Add(1)
				case errors.Is(err, ErrSoldOut):
					soldOut.Add(1)
				case errors.Is(err, ErrAlready):
					already.Add(1)
				default:
					t.Errorf("grab: %v", err)
				}
			}(u, try)
		}
	}
	wg.Wait()

	if success.Load() != 50 {
		t.Fatalf("success %d, want 50 (soldout %d already %d)", success.Load(), soldOut.Load(), already.Load())
	}
	levels, err := st.Levels(ctx, 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	sum := 0
	for _, n := range levels {
		if n < 0 {
			t.Fatalf("negative shard %v", levels)
		}
		sum += n
	}
	if sum != 0 {
		t.Fatalf("remaining %d levels %v", sum, levels)
	}
	users, err := st.UserCount(ctx, 1)
	if err != nil || users != 50 {
		t.Fatalf("users %d err %v", users, err)
	}
	// 流里必须正好 50 条，和成功次数一致：扣库存和入队是一起成功的。
	n, err := st.rdb.XLen(ctx, st.stream).Result()
	if err != nil || n != 50 {
		t.Fatalf("stream len %d err %v", n, err)
	}
}

func TestLuaReplayDoesNotDeductTwice(t *testing.T) {
	st, _ := newTestRedis(t)
	ctx := context.Background()
	if _, err := st.Init(ctx, 7, []int{2}); err != nil {
		t.Fatal(err)
	}
	p := GrabParams{
		CouponID: 7, UserID: 9, IdemKey: "same", OrderID: 100,
		ShardN: 1, ExpireAt: time.Now().Add(time.Minute), CreatedAt: time.Now(),
	}
	first, err := st.Grab(ctx, p)
	if err != nil || first.Replay {
		t.Fatalf("first %+v %v", first, err)
	}
	second, err := st.Grab(ctx, p)
	if err != nil || !second.Replay || second.OrderID != first.OrderID {
		t.Fatalf("replay %+v %v", second, err)
	}
	levels, _ := st.Levels(ctx, 7, 1)
	if levels[0] != 1 {
		t.Fatalf("replay deducted stock, left %v", levels)
	}
}

func TestCombinedNoOversell(t *testing.T) {
	st, _ := newTestRedis(t)
	ctx := context.Background()
	if _, err := st.Init(ctx, 11, Split(30, 4)); err != nil {
		t.Fatal(err)
	}
	var success atomic.Int64
	var wg sync.WaitGroup
	for u := int64(1); u <= 80; u++ {
		wg.Add(1)
		go func(user int64) {
			defer wg.Done()
			_, err := st.GrabCombined(ctx, GrabParams{
				CouponID: 11, UserID: user, IdemKey: "c" + itoa(user), OrderID: 1000 + user, ShardN: 4,
				ExpireAt: time.Now().Add(time.Minute), CreatedAt: time.Now(),
			}, 10, 10*time.Second, 0, 1)
			if err == nil {
				success.Add(1)
			} else if !errors.Is(err, ErrSoldOut) && !errors.Is(err, ErrAlready) {
				t.Errorf("grab %v", err)
			}
		}(u)
	}
	wg.Wait()
	if success.Load() != 30 {
		t.Fatalf("success %d", success.Load())
	}
	levels, _ := st.Levels(ctx, 11, 4)
	sum := 0
	for _, n := range levels {
		if n < 0 {
			t.Fatalf("negative %v", levels)
		}
		sum += n
	}
	if sum != 0 {
		t.Fatalf("left %d", sum)
	}
}

func TestCombinedReplayDoesNotConsumeLimit(t *testing.T) {
	st, _ := newTestRedis(t)
	ctx := context.Background()
	if _, err := st.Init(ctx, 9, []int{5}); err != nil {
		t.Fatal(err)
	}
	p := GrabParams{
		CouponID: 9, UserID: 3, IdemKey: "once", OrderID: 11, ShardN: 1,
		ExpireAt: time.Now().Add(time.Minute), CreatedAt: time.Now(),
	}
	if _, err := st.GrabCombined(ctx, p, 2, 10*time.Second, 0, 1); err != nil {
		t.Fatal(err)
	}
	// 换幂等键：第二次占满窗口并返回 already_owned，第三次必须是限流，库存不再变。
	p.IdemKey = "twice"
	p.OrderID = 12
	if _, err := st.GrabCombined(ctx, p, 2, 10*time.Second, 0, 1); !errors.Is(err, ErrAlready) {
		t.Fatalf("second %v", err)
	}
	p.IdemKey = "third"
	p.OrderID = 13
	if _, err := st.GrabCombined(ctx, p, 2, 10*time.Second, 0, 1); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("third %v", err)
	}
	// 原来的幂等键在窗口已满时仍然回放，并且不再扣库存。
	p.IdemKey = "once"
	p.OrderID = 11
	again, err := st.GrabCombined(ctx, p, 2, 10*time.Second, 0, 1)
	if err != nil || !again.Replay || again.OrderID != 11 {
		t.Fatalf("replay %+v %v", again, err)
	}
	levels, _ := st.Levels(ctx, 9, 1)
	if levels[0] != 4 {
		t.Fatalf("stock changed, left %v", levels)
	}
	if n, err := st.rdb.XLen(ctx, st.stream).Result(); err != nil || n != 1 {
		t.Fatalf("stream %d %v", n, err)
	}
	card, err := st.rdb.ZCard(ctx, UserRateKey(9, 3)).Result()
	if err != nil || card != 2 {
		t.Fatalf("window card %d err %v", card, err)
	}
}

func TestReturnIsIdempotent(t *testing.T) {
	st, _ := newTestRedis(t)
	ctx := context.Background()
	_, err := st.Init(ctx, 3, []int{1, 1})
	if err != nil {
		t.Fatal(err)
	}
	out, err := st.Grab(ctx, GrabParams{
		CouponID: 3, UserID: 4, IdemKey: "k", OrderID: 8, ShardN: 2,
		ExpireAt: time.Now().Add(time.Minute), CreatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ok, err := st.Return(ctx, 3, 4, out.Shard)
	if err != nil || !ok {
		t.Fatalf("return %v %v", ok, err)
	}
	ok, err = st.Return(ctx, 3, 4, out.Shard)
	if err != nil || ok {
		t.Fatalf("second return must be a no-op, ok=%v err=%v", ok, err)
	}
	levels, _ := st.Levels(ctx, 3, 2)
	sum := levels[0] + levels[1]
	if sum != 2 {
		t.Fatalf("stock after one return = %d, levels %v", sum, levels)
	}
}

func TestMoveRefusesToGoNegative(t *testing.T) {
	st, _ := newTestRedis(t)
	ctx := context.Background()
	_, err := st.Init(ctx, 5, []int{3, 0})
	if err != nil {
		t.Fatal(err)
	}
	ok, err := st.Move(ctx, 5, 0, 1, 2)
	if err != nil || !ok {
		t.Fatal(err)
	}
	ok, err = st.Move(ctx, 5, 0, 1, 5)
	if err != nil || ok {
		t.Fatalf("over-move ok=%v err=%v", ok, err)
	}
	levels, _ := st.Levels(ctx, 5, 2)
	if levels[0]+levels[1] != 3 {
		t.Fatalf("sum changed %v", levels)
	}
}

func itoa(n int64) string {
	return strconvI(n)
}

func strconvI(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

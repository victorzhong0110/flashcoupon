package integration

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/victorzhong0110/teabreak/internal/cache"
	"github.com/victorzhong0110/teabreak/internal/config"
	"github.com/victorzhong0110/teabreak/internal/httpserver"
	"github.com/victorzhong0110/teabreak/internal/idgen"
	"github.com/victorzhong0110/teabreak/internal/model"
	"github.com/victorzhong0110/teabreak/internal/order"
	"github.com/victorzhong0110/teabreak/internal/ratelimit"
	"github.com/victorzhong0110/teabreak/internal/reconcile"
	"github.com/victorzhong0110/teabreak/internal/seckill"
	"github.com/victorzhong0110/teabreak/internal/stock"
	"github.com/victorzhong0110/teabreak/internal/worker"
)

type stack struct {
	svc   *seckill.Service
	check *reconcile.Checker
	w     *worker.Worker
	rdb   *redis.Client
}

func newStack(t *testing.T) *stack {
	t.Helper()
	dsn := os.Getenv("MYSQL_DSN")
	addr := os.Getenv("REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("set MYSQL_DSN and REDIS_ADDR to run integration tests")
	}
	db, err := order.Open(dsn)
	if err != nil {
		t.Fatalf("mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := order.Migrate(db); err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 64})
	t.Cleanup(func() { _ = rdb.Close() })
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	stream := "tb:stream:it:" + name
	dlq := stream + ":dlq"
	group := "g-" + name
	// 清掉上一次同名测试留下的流，避免旧消息被新消费组从 0 再读一遍。
	_ = rdb.Del(context.Background(), stream, dlq).Err()

	repo := order.NewRepo(db)
	st := stock.New(rdb, stream, time.Hour)
	meta := cache.NewMeta(rdb, func(ctx context.Context, id int64) (*model.Coupon, error) {
		return repo.GetCoupon(ctx, id)
	}, cache.Options{SoftTTL: time.Minute, HardTTL: 2 * time.Minute, NegTTL: time.Second, Jitter: 0})
	cfg := config.Load()
	cfg.GlobalRPS = 0
	cfg.UserLimit = 0
	cfg.StreamKey = stream
	cfg.DLQKey = dlq
	cfg.Group = group
	svc := seckill.New(repo, st, meta, ratelimit.NewRedis(rdb), idgen.New(9), cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := &worker.Worker{
		RDB: rdb, Repo: repo, Stock: st,
		Stream: stream, DLQ: dlq, Group: group,
		Name: "it", Workers: 4, MaxDeliveries: 5,
		ClaimIdle: 300 * time.Millisecond, CancelEvery: 200 * time.Millisecond,
		RebalanceEvery: 0, RebalanceGap: 2,
	}
	go func() { _ = w.Run(ctx) }()
	waitGroup(t, rdb, stream, group)
	return &stack{
		svc:   svc,
		check: reconcile.New(repo, st, rdb, stream, dlq, group),
		w:     w,
		rdb:   rdb,
	}
}

func waitGroup(t *testing.T, rdb *redis.Client, stream, group string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		groups, err := rdb.XInfoGroups(context.Background(), stream).Result()
		if err == nil {
			for _, g := range groups {
				if g.Name == group {
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("consumer group was not created")
}

func (s *stack) coupon(t *testing.T, strategy model.Strategy, total, shards, timeoutSec int) *model.Coupon {
	t.Helper()
	if shards == 0 {
		shards = 1
	}
	c, err := s.svc.CreateCoupon(context.Background(), seckill.CreateInput{
		Name: "集成测试券", TotalStock: total, ShardCount: shards, Strategy: strategy,
		StartAt: time.Now().Add(-time.Minute), EndAt: time.Now().Add(time.Hour),
		PayTimeoutSec: timeoutSec,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func (s *stack) waitConsistent(t *testing.T, id int64) reconcile.Report {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var rep reconcile.Report
	var err error
	for time.Now().Before(deadline) {
		rep, err = s.check.Check(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if rep.Consistent {
			return rep
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("not consistent: %+v", rep)
	return rep
}

func TestNoOversellSharded(t *testing.T) {
	s := newStack(t)
	const stockN = 80
	const users = 400
	c := s.coupon(t, model.StrategySharded, stockN, 8, 3600)

	var okN, sold, already atomic.Int64
	var wg sync.WaitGroup
	for u := 1; u <= users; u++ {
		wg.Add(1)
		go func(u int) {
			defer wg.Done()
			_, err := s.svc.Grab(context.Background(), int64(u), c.ID, fmt.Sprintf("u%d", u))
			switch {
			case err == nil:
				okN.Add(1)
			case errors.Is(err, seckill.ErrSoldOut):
				sold.Add(1)
			case errors.Is(err, seckill.ErrAlready):
				already.Add(1)
			default:
				t.Errorf("grab user %d: %v", u, err)
			}
		}(u)
	}
	wg.Wait()
	if okN.Load() != stockN {
		t.Fatalf("accepted %d, want %d (soldout %d already %d)", okN.Load(), stockN, sold.Load(), already.Load())
	}
	rep := s.waitConsistent(t, c.ID)
	if rep.Oversell != 0 || rep.Active != stockN || rep.Snapshot.RedisRemaining != 0 || rep.Snapshot.DuplicateUsers != 0 {
		t.Fatalf("bad report %+v", rep)
	}
}

func TestOneUserAndIdempotency(t *testing.T) {
	s := newStack(t)
	c := s.coupon(t, model.StrategyRedis, 10, 1, 3600)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	results := make([]*seckill.GrabResult, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = s.svc.Grab(context.Background(), 7, c.ID, fmt.Sprintf("try-%d", i))
		}(i)
	}
	wg.Wait()
	success := 0
	winner := ""
	for i := 0; i < 2; i++ {
		if errs[i] == nil {
			success++
			winner = fmt.Sprintf("try-%d", i)
		} else if !errors.Is(errs[i], seckill.ErrAlready) {
			t.Fatalf("unexpected %v", errs[i])
		}
	}
	if success != 1 || winner == "" {
		t.Fatalf("success %d", success)
	}
	again, err := s.svc.Grab(context.Background(), 7, c.ID, winner)
	if err != nil || !again.Replay {
		t.Fatalf("replay %+v %v", again, err)
	}
	rep := s.waitConsistent(t, c.ID)
	if rep.Active != 1 {
		t.Fatalf("active %d", rep.Active)
	}
}

func TestTimeoutReturnsStock(t *testing.T) {
	s := newStack(t)
	c := s.coupon(t, model.StrategySharded, 1, 4, 1)
	// 生产上刚到期的订单走时间窗，不跟旧积压排队。测试仍指定券 id，
	// 这样断言不依赖库里还有没有别的过期单。
	s.w.SetCouponID(c.ID)
	res, err := s.svc.Grab(context.Background(), 3, c.ID, "only")
	if err != nil {
		t.Fatal(err)
	}
	s.waitConsistent(t, c.ID)
	// 支付超时 1 秒，取消循环 200ms。等到库存回来并且同一用户能再抢。
	deadline := time.Now().Add(8 * time.Second)
	var again *seckill.GrabResult
	for time.Now().Before(deadline) {
		again, err = s.svc.Grab(context.Background(), 3, c.ID, "second")
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("stock was not returned, last err %v order %d", err, res.OrderID)
	}
	rep := s.waitConsistent(t, c.ID)
	if rep.Active != 1 || rep.Snapshot.DBCancelled < 1 || rep.Oversell != 0 {
		t.Fatalf("%+v second order %d", rep, again.OrderID)
	}
}

func TestDBDirectNoOversell(t *testing.T) {
	s := newStack(t)
	const stockN = 20
	c := s.coupon(t, model.StrategyDB, stockN, 1, 3600)
	var okN atomic.Int64
	var wg sync.WaitGroup
	for u := 1; u <= 60; u++ {
		wg.Add(1)
		go func(u int) {
			defer wg.Done()
			_, err := s.svc.Grab(context.Background(), int64(u), c.ID, "db-"+strconv.Itoa(u))
			if err == nil {
				okN.Add(1)
			} else if !errors.Is(err, seckill.ErrSoldOut) && !errors.Is(err, seckill.ErrAlready) {
				t.Errorf("user %d: %v", u, err)
			}
		}(u)
	}
	wg.Wait()
	if okN.Load() != stockN {
		t.Fatalf("accepted %d want %d", okN.Load(), stockN)
	}
	rep := s.waitConsistent(t, c.ID)
	if rep.Oversell != 0 || rep.Active != stockN {
		t.Fatalf("%+v", rep)
	}
}

func TestUserRateLimit(t *testing.T) {
	s := newStack(t)
	s.svc.SetRateLimit(0, 0, 3, time.Second)
	c := s.coupon(t, model.StrategyRedis, 100, 1, 3600)
	limited := 0
	for i := 0; i < 6; i++ {
		_, err := s.svc.Grab(context.Background(), 99, c.ID, fmt.Sprintf("rl-%d", i))
		if errors.Is(err, seckill.ErrLimited) {
			limited++
		}
	}
	if limited == 0 {
		t.Fatal("rate limit did not trip")
	}
}

func TestHTTPGrabAndReconcile(t *testing.T) {
	s := newStack(t)
	c := s.coupon(t, model.StrategySharded, 5, 4, 3600)
	engine := httpserver.New(s.svc, s.check, nil, false)
	for u := 1; u <= 5; u++ {
		req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v1/coupons/%d/grab", c.ID), nil)
		req.Header.Set("X-User-Id", strconv.Itoa(u))
		req.Header.Set("Idempotency-Key", fmt.Sprintf("http-%d", u))
		rr := httptest.NewRecorder()
		engine.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status %d body %s", rr.Code, rr.Body.String())
		}
	}
	s.waitConsistent(t, c.ID)
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/coupons/%d/reconcile", c.ID), nil)
	rr := httptest.NewRecorder()
	engine.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"consistent":true`) {
		t.Fatalf("reconcile %d %s", rr.Code, rr.Body.String())
	}
	// 不存在的券 id 应该 404，而不是 500。
	req = httptest.NewRequest(http.MethodPost, "/v1/coupons/999999991/grab", nil)
	req.Header.Set("X-User-Id", "1")
	req.Header.Set("Idempotency-Key", "nope")
	rr = httptest.NewRecorder()
	engine.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("penetration status %d %s", rr.Code, rr.Body.String())
	}
}

func TestDLQAfterRetries(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")
	addr := os.Getenv("REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("set MYSQL_DSN and REDIS_ADDR to run integration tests")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	stream := "tb:stream:it:dlq:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	dlq := stream + ":dlq"
	group := "g-dlq"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &worker.Worker{
		RDB: rdb, Stream: stream, DLQ: dlq, Group: group,
		Name: "dlq", Workers: 1, MaxDeliveries: 2, ClaimIdle: 200 * time.Millisecond,
		// Repo 为空时 persist 会 panic。这个测试改走毒消息：缺字段，不碰数据库。
	}
	if err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"order_id": "not-a-number"},
	}).Err(); err != nil {
		t.Fatal(err)
	}
	go func() { _ = w.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n, err := rdb.XLen(ctx, dlq).Result()
		if err == nil && n >= 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("poison message was not moved to the DLQ")
}

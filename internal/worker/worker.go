// Package worker 是独立进程里的消费循环。
//
// 抢券接口只负责 Redis Lua。订单行在这里插入 MySQL。
// 这样做的原因：把行锁和磁盘 fsync 移出用户请求的临界路径。
//
// 消息可靠性靠三件事，而不是“发出去就不管了”：
//   - 生产：XADD 和扣库存在同一条 Lua 里，不存在“扣了库存但消息没写上”
//   - 消费：处理成功才 XACK；失败留在 PEL 里，过一会儿被 XAUTOCLAIM 重新投递
//   - 毒消息：投递次数达到上限就搬到死信流再 ACK，避免一条坏消息挡住后面的订单
//
// 重复投递是正常现象。orders 上的幂等唯一索引让第二次插入变成无害的 duplicate。
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/victorzhong0110/teabreak/internal/fulfill"
	"github.com/victorzhong0110/teabreak/internal/metrics"
	"github.com/victorzhong0110/teabreak/internal/model"
	"github.com/victorzhong0110/teabreak/internal/order"
	"github.com/victorzhong0110/teabreak/internal/stock"
)

type Worker struct {
	RDB               redis.UniversalClient
	Repo              *order.Repo
	Stock             *stock.RedisStock
	Stream            string
	DLQ               string
	Group             string
	Name              string
	Workers           int
	MaxDeliveries     int64
	ClaimIdle         time.Duration
	CancelEvery       time.Duration
	CancelParallel    int
	CancelBatch       int
	CancelFreshWindow time.Duration
	RebalanceEvery    time.Duration
	RebalanceGap      int
	// couponID > 0 时取消循环只处理这一张券，避免被别的券的过期积压挡住。
	// 用原子变量：测试在消费循环已经启动之后才知道券 id。
	couponID atomic.Int64
	Log      *slog.Logger
}

// SetCouponID 把取消循环收窄到一张券。0 表示全库按过期时间扫描。
func (w *Worker) SetCouponID(id int64) {
	w.couponID.Store(id)
}

func (w *Worker) Run(ctx context.Context) error {
	if w.Log == nil {
		w.Log = slog.Default()
	}
	if w.Name == "" {
		host, _ := os.Hostname()
		w.Name = host
	}
	if w.Workers < 1 {
		w.Workers = 1
	}
	var err error
	for i := 0; i < 30; i++ {
		err = w.ensureGroup(ctx)
		if err == nil || ctx.Err() != nil {
			break
		}
		// Redis 还在加载 AOF 时会回 LOADING。等它，而不是把进程直接打退出去。
		w.Log.Warn("ensure group", "err", err, "try", i)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for i := 0; i < w.Workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w.readLoop(ctx, fmt.Sprintf("%s-%d", w.Name, i))
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.claimLoop(ctx)
	}()
	if w.CancelEvery > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.cancelLoop(ctx)
		}()
	}
	if w.RebalanceEvery > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.rebalanceLoop(ctx)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.lagLoop(ctx)
	}()
	<-ctx.Done()
	wg.Wait()
	return nil
}

func (w *Worker) ensureGroup(ctx context.Context) error {
	// 从 0 开始，而不是 $。进程重启前已经写进流里的消息也要能被读到。
	err := w.RDB.XGroupCreateMkStream(ctx, w.Stream, w.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (w *Worker) readLoop(ctx context.Context, consumer string) {
	for ctx.Err() == nil {
		res, err := w.RDB.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    w.Group,
			Consumer: consumer,
			Streams:  []string{w.Stream, ">"},
			Count:    32,
			Block:    time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				continue
			}
			if strings.Contains(err.Error(), "NOGROUP") {
				_ = w.ensureGroup(ctx)
				continue
			}
			w.Log.Warn("xreadgroup", "err", err)
			sleep(ctx, 200*time.Millisecond)
			continue
		}
		for _, stream := range res {
			for _, msg := range stream.Messages {
				w.handle(ctx, msg, false)
			}
		}
	}
}

func (w *Worker) claimLoop(ctx context.Context) {
	idle := w.ClaimIdle
	if idle <= 0 {
		idle = 15 * time.Second
	}
	tick := time.NewTicker(idle / 2)
	if idle/2 < 100*time.Millisecond {
		tick.Reset(100 * time.Millisecond)
	}
	defer tick.Stop()
	start := "0-0"
	consumer := w.Name + "-claim"
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		msgs, next, err := w.RDB.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   w.Stream,
			Group:    w.Group,
			Consumer: consumer,
			MinIdle:  idle,
			Start:    start,
			Count:    32,
		}).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if !strings.Contains(err.Error(), "NOGROUP") && !strings.Contains(err.Error(), "no such key") {
				w.Log.Warn("xautoclaim", "err", err)
			}
			start = "0-0"
			continue
		}
		for _, msg := range msgs {
			w.handle(ctx, msg, true)
		}
		if next != "" {
			start = next
		} else {
			start = "0-0"
		}
	}
}

func (w *Worker) handle(ctx context.Context, msg redis.XMessage, fromClaim bool) {
	if fromClaim && w.deliveries(ctx, msg.ID) >= w.MaxDeliveries {
		if err := w.toDLQ(ctx, msg, "max deliveries"); err != nil {
			w.Log.Warn("dlq", "err", err, "id", msg.ID)
			return
		}
		w.ack(ctx, msg.ID)
		metrics.DLQTotal.Inc()
		metrics.ConsumeTotal.WithLabelValues("dlq").Inc()
		return
	}
	om, err := parseMessage(msg.Values)
	if err != nil {
		if derr := w.toDLQ(ctx, msg, err.Error()); derr != nil {
			w.Log.Warn("dlq poison", "err", derr)
			return
		}
		w.ack(ctx, msg.ID)
		metrics.DLQTotal.Inc()
		metrics.ConsumeTotal.WithLabelValues("poison").Inc()
		return
	}
	err = w.persist(ctx, om)
	if err != nil {
		metrics.ConsumeTotal.WithLabelValues("error").Inc()
		w.Log.Warn("persist", "err", err, "order", om.OrderID)
		return
	}
	w.ack(ctx, msg.ID)
}

func (w *Worker) persist(ctx context.Context, om model.OrderMessage) error {
	now := time.Now().UTC()
	o := &model.Order{
		ID:             om.OrderID,
		CouponID:       om.CouponID,
		UserID:         om.UserID,
		IdempotencyKey: om.IdemKey,
		Status:         model.StatusPending,
		ShardID:        om.Shard,
		CreatedAt:      om.CreatedAt,
		ExpireAt:       om.ExpireAt,
		UpdatedAt:      now,
	}
	err := w.Repo.InsertOrder(ctx, o)
	if err == nil {
		metrics.ConsumeTotal.WithLabelValues("inserted").Inc()
		return nil
	}
	if order.IsDuplicate(err) {
		// 唯一索引挡住了重复消费。这是成功，不是故障。
		metrics.ConsumeTotal.WithLabelValues("duplicate").Inc()
		return nil
	}
	return err
}

func (w *Worker) ack(ctx context.Context, id string) {
	if err := w.RDB.XAck(ctx, w.Stream, w.Group, id).Err(); err != nil && ctx.Err() == nil {
		w.Log.Warn("xack", "err", err, "id", id)
	}
}

func (w *Worker) toDLQ(ctx context.Context, msg redis.XMessage, reason string) error {
	values := map[string]interface{}{
		"origin_id": msg.ID,
		"reason":    reason,
	}
	for k, v := range msg.Values {
		values[k] = v
	}
	return w.RDB.XAdd(ctx, &redis.XAddArgs{Stream: w.DLQ, Values: values}).Err()
}

func (w *Worker) deliveries(ctx context.Context, id string) int64 {
	rows, err := w.RDB.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: w.Stream,
		Group:  w.Group,
		Start:  id,
		End:    id,
		Count:  1,
	}).Result()
	if err != nil || len(rows) == 0 {
		return 0
	}
	return rows[0].RetryCount
}

func (w *Worker) cancelLoop(ctx context.Context) {
	n := w.CancelParallel
	if n < 1 {
		n = 4
	}
	if n > 16 {
		n = 16
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 偶数协程只领刚到期的窗口，奇数协程从最老的 expire_at 清积压。
			w.cancelDrain(ctx, i%2 == 0)
		}(i)
	}
	wg.Wait()
}

func (w *Worker) cancelDrain(ctx context.Context, fresh bool) {
	batch := w.CancelBatch
	if batch <= 0 {
		batch = 200
	}
	window := w.CancelFreshWindow
	if window <= 0 {
		window = 3 * time.Second
	}
	idle := 50 * time.Millisecond
	if w.CancelEvery > 0 && w.CancelEvery < idle {
		idle = w.CancelEvery
	}
	for ctx.Err() == nil {
		win := time.Duration(0)
		if fresh {
			win = window
		}
		rows, err := w.Repo.ClaimCancelExpired(ctx, w.couponID.Load(), batch, win)
		if err != nil {
			w.Log.Warn("claim expired", "err", err, "fresh", fresh)
			if !wait(ctx, 200*time.Millisecond) {
				return
			}
			continue
		}
		for _, o := range rows {
			if err := fulfill.FinishCancel(ctx, w.Repo, w.Stock, o); err != nil {
				w.Log.Warn("return stock", "err", err, "order", o.ID)
			}
		}
		if fresh && len(rows) == 0 {
			if err := fulfill.RepairReturns(ctx, w.Repo, w.Stock, batch); err != nil {
				w.Log.Warn("repair returns", "err", err)
			}
		}
		if len(rows) == 0 {
			if !wait(ctx, idle) {
				return
			}
		}
	}
}

func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *Worker) rebalanceLoop(ctx context.Context) {
	tick := time.NewTicker(w.RebalanceEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			w.rebalanceOnce(ctx)
		}
	}
}

func (w *Worker) rebalanceOnce(ctx context.Context) {
	coupons, err := w.Repo.ListCoupons(ctx)
	if err != nil {
		w.Log.Warn("rebalance list", "err", err)
		return
	}
	now := time.Now()
	for _, c := range coupons {
		if c.Strategy != model.StrategySharded || c.ShardCount < 2 {
			continue
		}
		if now.Before(c.StartAt) || !now.Before(c.EndAt) {
			continue
		}
		levels, err := w.Stock.Levels(ctx, c.ID, c.ShardCount)
		if err != nil {
			continue
		}
		src, dst, amount, ok := stock.Plan(levels, w.RebalanceGap)
		if !ok {
			continue
		}
		moved, err := w.Stock.Move(ctx, c.ID, src, dst, amount)
		if err != nil {
			w.Log.Warn("rebalance move", "err", err, "coupon", c.ID)
			continue
		}
		if moved {
			metrics.RebalanceTotal.Inc()
			w.Log.Info("rebalance", "coupon", c.ID, "src", src, "dst", dst, "amount", amount)
		}
	}
}

func (w *Worker) lagLoop(ctx context.Context) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			groups, err := w.RDB.XInfoGroups(ctx, w.Stream).Result()
			if err != nil {
				continue
			}
			for _, g := range groups {
				if g.Name == w.Group {
					metrics.MQPending.Set(float64(g.Pending))
					metrics.MQLag.Set(float64(g.Lag))
				}
			}
		}
	}
}

func parseMessage(values map[string]interface{}) (model.OrderMessage, error) {
	orderID, err := strconv.ParseInt(asString(values["order_id"]), 10, 64)
	if err != nil {
		return model.OrderMessage{}, fmt.Errorf("order_id: %w", err)
	}
	userID, err := strconv.ParseInt(asString(values["user_id"]), 10, 64)
	if err != nil {
		return model.OrderMessage{}, fmt.Errorf("user_id: %w", err)
	}
	couponID, err := strconv.ParseInt(asString(values["coupon_id"]), 10, 64)
	if err != nil {
		return model.OrderMessage{}, fmt.Errorf("coupon_id: %w", err)
	}
	shard, err := strconv.Atoi(asString(values["shard"]))
	if err != nil {
		return model.OrderMessage{}, fmt.Errorf("shard: %w", err)
	}
	expMs, err := strconv.ParseInt(asString(values["expire_at_ms"]), 10, 64)
	if err != nil {
		return model.OrderMessage{}, fmt.Errorf("expire_at_ms: %w", err)
	}
	createdMs, err := strconv.ParseInt(asString(values["created_at_ms"]), 10, 64)
	if err != nil {
		return model.OrderMessage{}, fmt.Errorf("created_at_ms: %w", err)
	}
	idem := asString(values["idem"])
	if idem == "" {
		return model.OrderMessage{}, errors.New("empty idem")
	}
	return model.OrderMessage{
		OrderID:   orderID,
		UserID:    userID,
		CouponID:  couponID,
		IdemKey:   idem,
		Shard:     shard,
		ExpireAt:  time.UnixMilli(expMs).UTC(),
		CreatedAt: time.UnixMilli(createdMs).UTC(),
	}, nil
}

func asString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case nil:
		return ""
	default:
		return fmt.Sprint(t)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

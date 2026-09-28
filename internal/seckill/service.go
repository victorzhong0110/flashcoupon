// Package seckill 是抢券的应用层。
// HTTP、消费者、对账都从这里拿同一套规则：活动时间、限流、一人一单、库存路径。
package seckill

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/victorzhong0110/teabreak/internal/cache"
	"github.com/victorzhong0110/teabreak/internal/config"
	"github.com/victorzhong0110/teabreak/internal/fulfill"
	"github.com/victorzhong0110/teabreak/internal/idgen"
	"github.com/victorzhong0110/teabreak/internal/metrics"
	"github.com/victorzhong0110/teabreak/internal/model"
	"github.com/victorzhong0110/teabreak/internal/order"
	"github.com/victorzhong0110/teabreak/internal/ratelimit"
	"github.com/victorzhong0110/teabreak/internal/stock"
)

var (
	ErrSoldOut     = stock.ErrSoldOut
	ErrAlready     = stock.ErrAlready
	ErrNotFound    = cache.ErrNotFound
	ErrLimited     = stock.ErrRateLimited
	ErrUnavailable = errors.New("unavailable")
	ErrNotStarted  = errors.New("not_started")
	ErrEnded       = errors.New("ended")
	ErrBadRequest  = errors.New("bad_request")
	ErrNotPending  = errors.New("not_pending")
)

var idemPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)

type GrabResult struct {
	OrderID  int64  `json:"order_id"`
	CouponID int64  `json:"coupon_id"`
	UserID   int64  `json:"user_id"`
	Status   string `json:"status"`
	Shard    int    `json:"shard"`
	Replay   bool   `json:"replay"`
	Strategy string `json:"strategy"`
}

type CreateInput struct {
	Name          string
	TotalStock    int
	ShardCount    int
	Strategy      model.Strategy
	StartAt       time.Time
	EndAt         time.Time
	PayTimeoutSec int
}

type Service struct {
	repo  *order.Repo
	stock *stock.RedisStock
	cache *cache.Meta
	limit *ratelimit.RedisLimiter
	ids   *idgen.Generator
	cfg   config.Config
}

func New(repo *order.Repo, st *stock.RedisStock, meta *cache.Meta, limit *ratelimit.RedisLimiter, ids *idgen.Generator, cfg config.Config) *Service {
	return &Service{repo: repo, stock: st, cache: meta, limit: limit, ids: ids, cfg: cfg}
}

func (s *Service) Repo() *order.Repo        { return s.repo }
func (s *Service) Stock() *stock.RedisStock { return s.stock }
func (s *Service) Cache() *cache.Meta       { return s.cache }

// SetRateLimit 给压测和测试改限流。GlobalRPS<=0 或 UserLimit<=0 表示关闭那一层。
func (s *Service) SetRateLimit(globalRPS float64, burst, userLimit int, window time.Duration) {
	s.cfg.GlobalRPS = globalRPS
	s.cfg.GlobalBurst = burst
	s.cfg.UserLimit = userLimit
	s.cfg.UserWindow = window
}

// CreateCoupon 写数据库，再把库存铺到 Redis。
// 铺库存放在插入之后：即使进程在这一步崩溃，下次预热发现没有 warmed 标记，
// 会按“总库存 - 已落库的有效订单”把 Redis 重建出来。
func (s *Service) CreateCoupon(ctx context.Context, in CreateInput) (*model.Coupon, error) {
	if in.PayTimeoutSec <= 0 {
		in.PayTimeoutSec = 900
	}
	if err := validateCreate(in); err != nil {
		return nil, err
	}
	c := &model.Coupon{
		Name:          in.Name,
		TotalStock:    in.TotalStock,
		ShardCount:    in.ShardCount,
		Strategy:      in.Strategy,
		StartAt:       in.StartAt.UTC(),
		EndAt:         in.EndAt.UTC(),
		PayTimeoutSec: in.PayTimeoutSec,
		PerUserLimit:  1,
	}
	if err := s.repo.CreateCoupon(ctx, c); err != nil {
		return nil, err
	}
	// 先放进布隆过滤器，再对外可见。否则创建完成到 Remember 之间的请求会被当成穿透。
	s.cache.Remember(c)
	if err := s.cache.PutRedis(ctx, c); err != nil {
		return nil, err
	}
	if c.Strategy != model.StrategyDB {
		if _, err := s.stock.Init(ctx, c.ID, stock.Split(c.TotalStock, c.ShardCount)); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func validateCreate(in CreateInput) error {
	if in.Name == "" || len(in.Name) > 128 {
		return fmt.Errorf("%w: name", ErrBadRequest)
	}
	if in.TotalStock <= 0 {
		return fmt.Errorf("%w: total_stock", ErrBadRequest)
	}
	if in.PayTimeoutSec <= 0 {
		return fmt.Errorf("%w: pay_timeout_sec", ErrBadRequest)
	}
	switch in.Strategy {
	case model.StrategyDB, model.StrategyRedis:
		if in.ShardCount != 1 {
			return fmt.Errorf("%w: %s strategy requires shard_count=1", ErrBadRequest, in.Strategy)
		}
	case model.StrategySharded:
		if in.ShardCount < 2 || in.ShardCount > 64 {
			return fmt.Errorf("%w: sharded strategy requires shard_count 2..64", ErrBadRequest)
		}
	default:
		return fmt.Errorf("%w: strategy must be db, redis or sharded", ErrBadRequest)
	}
	if !in.EndAt.After(in.StartAt) {
		return fmt.Errorf("%w: end_at must be after start_at", ErrBadRequest)
	}
	return nil
}

func (s *Service) GetCoupon(ctx context.Context, id int64) (*model.Coupon, error) {
	return s.cache.Get(ctx, id)
}

func (s *Service) ListCoupons(ctx context.Context) ([]model.Coupon, error) {
	return s.repo.ListCoupons(ctx)
}

// Grab 是热路径。幂等回放放在限流前面：客户端超时重试不该被自己的限流挡住，
// 否则用户会以为没抢到，实际上库存已经扣了。
func (s *Service) Grab(ctx context.Context, userID, couponID int64, idemKey string) (res *GrabResult, err error) {
	start := time.Now()
	strategy := "unknown"
	defer func() {
		result := "ok"
		if err != nil {
			result = errorName(err)
		} else if res != nil && res.Replay {
			result = "replay"
		}
		metrics.GrabTotal.WithLabelValues(result, strategy).Inc()
		metrics.GrabDuration.WithLabelValues(strategy).Observe(time.Since(start).Seconds())
	}()

	if userID <= 0 || !idemPattern.MatchString(idemKey) {
		return nil, fmt.Errorf("%w: need X-User-Id and Idempotency-Key", ErrBadRequest)
	}
	c, err := s.cache.Get(ctx, couponID)
	if err != nil {
		return nil, err
	}
	strategy = string(c.Strategy)
	now := time.Now()
	if now.Before(c.StartAt) {
		return nil, ErrNotStarted
	}
	if !now.Before(c.EndAt) {
		return nil, ErrEnded
	}

	// 合并热路径时，回放、全局限流、用户限流和扣库存在同一条 Lua 里。
	// 回放在脚本开头返回，不会消耗令牌。
	if c.Strategy != model.StrategyDB && s.cfg.CombinedHotpath {
		return s.grabRedis(ctx, c, userID, idemKey, now)
	}

	if replay, ok, err := s.lookupReplay(ctx, c, userID, idemKey); err != nil {
		return nil, err
	} else if ok {
		return replay, nil
	}

	if s.limit != nil && s.cfg.GlobalRPS > 0 {
		ok, err := s.limit.AllowGlobal(ctx, s.cfg.GlobalRPS, s.cfg.GlobalBurst)
		if err != nil {
			return nil, fmt.Errorf("%w: global limiter: %v", ErrUnavailable, err)
		}
		if !ok {
			return nil, ErrLimited
		}
	}
	if s.limit != nil && s.cfg.UserLimit > 0 {
		ok, err := s.limit.AllowUser(ctx, couponID, userID, s.cfg.UserLimit, s.cfg.UserWindow)
		if err != nil {
			return nil, fmt.Errorf("%w: user limiter: %v", ErrUnavailable, err)
		}
		if !ok {
			return nil, ErrLimited
		}
	}

	if c.Strategy == model.StrategyDB {
		return s.grabDB(ctx, c, userID, idemKey, now)
	}
	return s.grabRedis(ctx, c, userID, idemKey, now)
}

func (s *Service) lookupReplay(ctx context.Context, c *model.Coupon, userID int64, idemKey string) (*GrabResult, bool, error) {
	if c.Strategy == model.StrategyDB {
		existing, err := s.repo.GetOrderByIdem(ctx, c.ID, idemKey)
		if err != nil {
			return nil, false, err
		}
		if existing == nil {
			return nil, false, nil
		}
		if existing.UserID != userID {
			return nil, false, ErrAlready
		}
		res := resultFromOrder(existing, true)
		res.Strategy = string(c.Strategy)
		return res, true, nil
	}
	raw, err := s.stock.GetIdem(ctx, c.ID, idemKey)
	if err != nil || raw == "" {
		return nil, false, err
	}
	out, err := stock.ParsePayload(raw)
	if err != nil {
		return nil, false, err
	}
	return &GrabResult{
		OrderID: out.OrderID, CouponID: c.ID, UserID: out.UserID,
		Status: string(model.StatusPending), Shard: out.Shard, Replay: true,
		Strategy: string(c.Strategy),
	}, true, nil
}

func (s *Service) grabRedis(ctx context.Context, c *model.Coupon, userID int64, idemKey string, now time.Time) (*GrabResult, error) {
	params := stock.GrabParams{
		CouponID:  c.ID,
		UserID:    userID,
		IdemKey:   idemKey,
		OrderID:   s.ids.Next(),
		ShardN:    c.ShardCount,
		ExpireAt:  now.Add(time.Duration(c.PayTimeoutSec) * time.Second),
		CreatedAt: now,
	}
	var (
		out stock.GrabOutcome
		err error
	)
	if s.cfg.CombinedHotpath {
		out, err = s.stock.GrabCombined(ctx, params, s.cfg.UserLimit, s.cfg.UserWindow, s.cfg.GlobalRPS, s.cfg.GlobalBurst)
	} else {
		out, err = s.stock.Grab(ctx, params)
	}
	if err != nil {
		return nil, err
	}
	return &GrabResult{
		OrderID: out.OrderID, CouponID: c.ID, UserID: userID,
		Status: string(model.StatusPending), Shard: out.Shard, Replay: out.Replay,
		Strategy: string(c.Strategy),
	}, nil
}

func (s *Service) grabDB(ctx context.Context, c *model.Coupon, userID int64, idemKey string, now time.Time) (*GrabResult, error) {
	o := &model.Order{
		ID:             s.ids.Next(),
		CouponID:       c.ID,
		UserID:         userID,
		IdempotencyKey: idemKey,
		Status:         model.StatusPending,
		ShardID:        -1,
		CreatedAt:      now,
		ExpireAt:       now.Add(time.Duration(c.PayTimeoutSec) * time.Second),
		UpdatedAt:      now,
	}
	err := s.repo.GrabDB(ctx, o)
	if err == nil {
		res := resultFromOrder(o, false)
		res.Strategy = string(c.Strategy)
		return res, nil
	}
	if errors.Is(err, order.ErrSoldOut) {
		return nil, ErrSoldOut
	}
	if order.IsDuplicate(err) {
		existing, gerr := s.repo.GetOrderByIdem(ctx, c.ID, idemKey)
		if gerr != nil {
			return nil, gerr
		}
		if existing != nil && existing.UserID == userID {
			res := resultFromOrder(existing, true)
			res.Strategy = string(c.Strategy)
			return res, nil
		}
		return nil, ErrAlready
	}
	return nil, err
}

func (s *Service) Confirm(ctx context.Context, orderID int64) (*model.Order, error) {
	ok, err := s.repo.Confirm(ctx, orderID)
	if err != nil {
		return nil, err
	}
	o, err := s.repo.GetOrder(ctx, orderID)
	if err != nil {
		return nil, err
	}
	if o == nil {
		return nil, ErrNotFound
	}
	if !ok && o.Status != model.StatusConfirmed {
		return nil, ErrNotPending
	}
	return o, nil
}

func (s *Service) Cancel(ctx context.Context, orderID int64) (*model.Order, error) {
	ok, err := fulfill.CancelPending(ctx, s.repo, s.stock, orderID)
	if err != nil {
		return nil, err
	}
	o, gerr := s.repo.GetOrder(ctx, orderID)
	if gerr != nil {
		return nil, gerr
	}
	if o == nil {
		return nil, ErrNotFound
	}
	if !ok {
		return nil, ErrNotPending
	}
	return o, nil
}

// Warmup 在监听端口之前跑。
// Redis 里已经有 warmed 标记就只刷新本地缓存；标记丢了（例如 Redis 没开 AOF 被清空）
// 才按数据库里的有效订单重建库存，避免把已卖出的量又加回去造成超卖。
func (s *Service) Warmup(ctx context.Context) error {
	coupons, err := s.repo.ListCoupons(ctx)
	if err != nil {
		return err
	}
	for i := range coupons {
		c := coupons[i]
		s.cache.Remember(&c)
		if err := s.cache.PutRedis(ctx, &c); err != nil {
			return err
		}
		if c.Strategy == model.StrategyDB {
			continue
		}
		warmed, err := s.stock.Warmed(ctx, c.ID)
		if err != nil {
			return err
		}
		if warmed {
			continue
		}
		users, err := s.repo.ListActiveUserIDs(ctx, c.ID)
		if err != nil {
			return err
		}
		remain := c.TotalStock - len(users)
		if remain < 0 {
			remain = 0
		}
		if _, err := s.stock.Init(ctx, c.ID, stock.Split(remain, c.ShardCount)); err != nil {
			return err
		}
		if err := s.stock.AddUsers(ctx, c.ID, users); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) RefreshStockGauges(ctx context.Context) {
	coupons, err := s.repo.ListCoupons(ctx)
	if err != nil {
		return
	}
	for _, c := range coupons {
		label := strconv.FormatInt(c.ID, 10)
		if c.Strategy == model.StrategyDB {
			metrics.StockRemaining.WithLabelValues(label).Set(float64(c.Remaining))
			continue
		}
		levels, err := s.stock.Levels(ctx, c.ID, c.ShardCount)
		if err != nil {
			continue
		}
		sum := 0
		for _, n := range levels {
			sum += n
		}
		metrics.StockRemaining.WithLabelValues(label).Set(float64(sum))
	}
}

func (s *Service) StockLevels(ctx context.Context, id int64) ([]int, error) {
	c, err := s.cache.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if c.Strategy == model.StrategyDB {
		n, err := s.repo.DBRemaining(ctx, id)
		if err != nil {
			return nil, err
		}
		return []int{n}, nil
	}
	return s.stock.Levels(ctx, id, c.ShardCount)
}

func resultFromOrder(o *model.Order, replay bool) *GrabResult {
	return &GrabResult{
		OrderID: o.ID, CouponID: o.CouponID, UserID: o.UserID,
		Status: string(o.Status), Shard: o.ShardID, Replay: replay,
	}
}

func errorName(err error) string {
	switch {
	case errors.Is(err, ErrSoldOut):
		return "sold_out"
	case errors.Is(err, ErrAlready):
		return "already_owned"
	case errors.Is(err, ErrLimited):
		return "rate_limited"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrNotStarted):
		return "not_started"
	case errors.Is(err, ErrEnded):
		return "ended"
	case errors.Is(err, ErrBadRequest):
		return "bad_request"
	case errors.Is(err, ErrUnavailable):
		return "unavailable"
	default:
		return "error"
	}
}

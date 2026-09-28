// Package cache 挡住券模板读请求，不参与库存数字。
// 库存必须走 Lua；这里缓存的是名称、活动时间、分桶数这类很少变的字段。
//
// 三种经典问题在这个文件里各有一个具体做法：
//
//   - 穿透：布隆过滤器直接拒绝从没创建过的 id，不给数据库制造随机点查。
//   - 击穿：同一个 id 并发回源时用 singleflight 合成一次加载；过期后先返回旧值再后台刷新。
//   - 雪崩：本地 TTL 加抖动。Redis 里的模板 hash 不设统一 TTL，避免同一秒集体失效。
package cache

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/victorzhong0110/teabreak/internal/metrics"
	"github.com/victorzhong0110/teabreak/internal/model"
	"github.com/victorzhong0110/teabreak/internal/stock"
	"golang.org/x/sync/singleflight"
)

var ErrNotFound = errors.New("coupon_not_found")

type Loader func(ctx context.Context, id int64) (*model.Coupon, error)

type Options struct {
	SoftTTL time.Duration
	HardTTL time.Duration
	NegTTL  time.Duration
	Jitter  float64 // 0.2 表示在 TTL 上再随机加 0%~20%
}

type Meta struct {
	rdb   redis.UniversalClient
	load  Loader
	opt   Options
	mu    sync.RWMutex
	items map[int64]entry
	bloom *Bloom
	sf    singleflight.Group
	// dbLoads 给测试看“这次 Get 有没有打到 Loader”。
	dbLoads int64
	loadMu  sync.Mutex
}

type entry struct {
	coupon   *model.Coupon
	negative bool
	softExp  time.Time
	hardExp  time.Time
}

func NewMeta(rdb redis.UniversalClient, load Loader, opt Options) *Meta {
	if opt.SoftTTL <= 0 {
		opt.SoftTTL = 30 * time.Second
	}
	if opt.HardTTL <= 0 {
		opt.HardTTL = 2 * time.Minute
	}
	if opt.NegTTL <= 0 {
		opt.NegTTL = 20 * time.Second
	}
	if opt.Jitter <= 0 {
		opt.Jitter = 0.2
	}
	return &Meta{
		rdb:   rdb,
		load:  load,
		opt:   opt,
		items: make(map[int64]entry),
		bloom: NewBloom(1<<20, 4),
	}
}

func (m *Meta) DBLoads() int64 {
	m.loadMu.Lock()
	defer m.loadMu.Unlock()
	return m.dbLoads
}

// Remember 在创建券或预热时调用，让布隆过滤器和本地缓存立刻认识这张券。
func (m *Meta) Remember(c *model.Coupon) {
	if c == nil {
		return
	}
	m.bloom.Add(c.ID)
	m.store(c)
}

func (m *Meta) Get(ctx context.Context, id int64) (*model.Coupon, error) {
	now := time.Now()
	m.mu.RLock()
	e, hit := m.items[id]
	m.mu.RUnlock()
	if hit && now.Before(e.hardExp) {
		if e.negative && now.Before(e.softExp) {
			// 空值缓存：这个 id 刚刚查过，数据库里没有。挡住穿透的第二层。
			metrics.CacheLoadTotal.WithLabelValues("negative").Inc()
			return nil, ErrNotFound
		}
		if !e.negative && now.Before(e.softExp) {
			metrics.CacheLoadTotal.WithLabelValues("local").Inc()
			return clone(e.coupon), nil
		}
		if !e.negative {
			// 逻辑过期：先把旧值交给这次请求，刷新放到后台，避免热点 key 到期瞬间打穿数据库。
			metrics.CacheLoadTotal.WithLabelValues("stale").Inc()
			go m.refresh(id)
			return clone(e.coupon), nil
		}
	}
	if !m.bloom.MayContain(id) {
		metrics.CacheLoadTotal.WithLabelValues("bloom").Inc()
		// 布隆过滤器是进程内的。另一台 API 刚创建的券，这台的过滤器里没有，
		// 但 Redis 里已经有模板。先问 Redis，避免把真实的券打成 404。
		// Redis 也没有时才拒绝，随机 id 仍然进不了 MySQL。
		if m.rdb != nil {
			c, err := m.readRedis(ctx, id)
			if err != nil {
				return nil, err
			}
			if c != nil {
				metrics.CacheLoadTotal.WithLabelValues("redis").Inc()
				m.bloom.Add(c.ID)
				m.store(c)
				return clone(c), nil
			}
		}
		return nil, ErrNotFound
	}
	v, err, _ := m.sf.Do(strconv.FormatInt(id, 10), func() (interface{}, error) {
		return m.loadOne(context.Background(), id)
	})
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, ErrNotFound
	}
	return clone(v.(*model.Coupon)), nil
}

func (m *Meta) refresh(id int64) {
	_, _, _ = m.sf.Do("r"+strconv.FormatInt(id, 10), func() (interface{}, error) {
		return m.loadOne(context.Background(), id)
	})
}

func (m *Meta) loadOne(ctx context.Context, id int64) (*model.Coupon, error) {
	if m.rdb != nil {
		c, err := m.readRedis(ctx, id)
		if err != nil {
			return nil, err
		}
		if c != nil {
			metrics.CacheLoadTotal.WithLabelValues("redis").Inc()
			m.bloom.Add(c.ID)
			m.store(c)
			return c, nil
		}
	}
	m.loadMu.Lock()
	m.dbLoads++
	m.loadMu.Unlock()
	c, err := m.load(ctx, id)
	if err != nil {
		return nil, err
	}
	if c == nil {
		metrics.CacheLoadTotal.WithLabelValues("negative").Inc()
		ttl := m.jitter(m.opt.NegTTL)
		until := time.Now().Add(ttl)
		m.mu.Lock()
		m.items[id] = entry{negative: true, softExp: until, hardExp: until}
		m.mu.Unlock()
		return nil, nil
	}
	metrics.CacheLoadTotal.WithLabelValues("db").Inc()
	if err := m.writeRedis(ctx, c); err != nil {
		return nil, err
	}
	m.bloom.Add(c.ID)
	m.store(c)
	return c, nil
}

func (m *Meta) readRedis(ctx context.Context, id int64) (*model.Coupon, error) {
	vals, err := m.rdb.HGetAll(ctx, stock.MetaKey(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, nil
	}
	return couponFromHash(id, vals), nil
}

func (m *Meta) writeRedis(ctx context.Context, c *model.Coupon) error {
	if m.rdb == nil || c == nil {
		return nil
	}
	return m.rdb.HSet(ctx, stock.MetaKey(c.ID), hashFromCoupon(c)).Err()
}

// PutRedis 给创建券的路径写模板，和库存初始化分开，方便各自重试。
func (m *Meta) PutRedis(ctx context.Context, c *model.Coupon) error {
	return m.writeRedis(ctx, c)
}

func (m *Meta) store(c *model.Coupon) {
	now := time.Now()
	m.mu.Lock()
	m.items[c.ID] = entry{
		coupon:  clone(c),
		softExp: now.Add(m.jitter(m.opt.SoftTTL)),
		hardExp: now.Add(m.jitter(m.opt.HardTTL)),
	}
	m.mu.Unlock()
}

func (m *Meta) jitter(d time.Duration) time.Duration {
	if m.opt.Jitter <= 0 {
		return d
	}
	extra := time.Duration(float64(d) * m.opt.Jitter * rand.Float64())
	return d + extra
}

func clone(c *model.Coupon) *model.Coupon {
	if c == nil {
		return nil
	}
	cp := *c
	return &cp
}

func hashFromCoupon(c *model.Coupon) map[string]interface{} {
	return map[string]interface{}{
		"name":            c.Name,
		"total_stock":     c.TotalStock,
		"shard_count":     c.ShardCount,
		"remaining":       c.Remaining,
		"strategy":        string(c.Strategy),
		"start_at":        c.StartAt.UTC().UnixMilli(),
		"end_at":          c.EndAt.UTC().UnixMilli(),
		"pay_timeout_sec": c.PayTimeoutSec,
		"per_user_limit":  c.PerUserLimit,
	}
}

func couponFromHash(id int64, v map[string]string) *model.Coupon {
	return &model.Coupon{
		ID:            id,
		Name:          v["name"],
		TotalStock:    atoi(v["total_stock"]),
		ShardCount:    atoi(v["shard_count"]),
		Remaining:     atoi(v["remaining"]),
		Strategy:      model.Strategy(v["strategy"]),
		StartAt:       time.UnixMilli(atoi64(v["start_at"])).UTC(),
		EndAt:         time.UnixMilli(atoi64(v["end_at"])).UTC(),
		PayTimeoutSec: atoi(v["pay_timeout_sec"]),
		PerUserLimit:  atoi(v["per_user_limit"]),
	}
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

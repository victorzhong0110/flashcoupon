// Package model 放券和订单的纯数据结构，不依赖 Redis 或 MySQL。
// 状态机先落在这些类型上，存储细节在后面的包里。
package model

import "time"

// Strategy 是扣库存的路径。三种路径共用同一套 HTTP 和订单表，
// 方便在同一台机器上做前后对比压测。
type Strategy string

const (
	// StrategyDB 在请求线程里用事务更新 coupons.remaining，作为慢基线。
	StrategyDB Strategy = "db"
	// StrategyRedis 单个库存 key + Lua。
	StrategyRedis Strategy = "redis"
	// StrategySharded 把库存拆到多个 key，仍然在一条 Lua 里原子扣减。
	StrategySharded Strategy = "sharded"
)

type OrderStatus string

const (
	StatusPending   OrderStatus = "PENDING"
	StatusConfirmed OrderStatus = "CONFIRMED"
	StatusCancelled OrderStatus = "CANCELLED"
)

// Coupon 是券模板。TotalStock 创建后不变，是对账的右边常数。
// Remaining 只给 DB 直写路径用；Redis 路径的真实余量在分桶 key 里。
type Coupon struct {
	ID            int64
	Name          string
	TotalStock    int
	ShardCount    int
	Remaining     int
	Strategy      Strategy
	StartAt       time.Time
	EndAt         time.Time
	PayTimeoutSec int
	PerUserLimit  int
	CreatedAt     time.Time
}

// Order 是已经（或即将）落库的一行。
// ShardID < 0 表示这单走的是 DB 直写，取消时把 coupons.remaining 加回去，
// 而不是去动 Redis。
type Order struct {
	ID             int64
	CouponID       int64
	UserID         int64
	IdempotencyKey string
	Status         OrderStatus
	ShardID        int
	StockReturned  bool
	CreatedAt      time.Time
	ExpireAt       time.Time
	UpdatedAt      time.Time
}

// OrderMessage 是 Lua 写入 Redis Stream 的字段，消费者只认这一份。
type OrderMessage struct {
	OrderID   int64
	UserID    int64
	CouponID  int64
	IdemKey   string
	Shard     int
	ExpireAt  time.Time
	CreatedAt time.Time
}

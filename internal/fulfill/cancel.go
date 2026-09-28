// Package fulfill 把“订单状态”和“库存归还”接在一起。
// 取消分成两步，是因为 MySQL 和 Redis 没有共同的事务：
//  1. 先把 PENDING 改成 CANCELLED（只有一个并发取消能成功）
//  2. 再执行幂等的还库存 Lua
//
// 如果进程死在两步之间，stock_returned 仍是 0，修复循环会再跑一次 Lua。
// Lua 用 SREM 的结果决定要不要 INCR，所以修复不会把库存加两次。
package fulfill

import (
	"context"

	"github.com/victorzhong0110/teabreak/internal/metrics"
	"github.com/victorzhong0110/teabreak/internal/model"
	"github.com/victorzhong0110/teabreak/internal/order"
	"github.com/victorzhong0110/teabreak/internal/stock"
)

// CancelPending 取消一张仍未确认的订单，并归还它占用的库存。
// 返回 false 表示订单不存在或已经不是 PENDING（例如刚被确认）。
func CancelPending(ctx context.Context, repo *order.Repo, st *stock.RedisStock, id int64) (bool, error) {
	o, ok, err := repo.CancelIfPending(ctx, id)
	if err != nil || !ok {
		return ok, err
	}
	if o.ShardID < 0 {
		// DB 直写路径的 remaining 已经在同一个 SQL 事务里加回去了。
		metrics.CancelTotal.WithLabelValues("db").Inc()
		return true, nil
	}
	if err := returnRedis(ctx, repo, st, o.CouponID, o.UserID, o.ShardID, o.ID); err != nil {
		metrics.CancelTotal.WithLabelValues("return_error").Inc()
		return true, err
	}
	metrics.CancelTotal.WithLabelValues("returned").Inc()
	return true, nil
}

// FinishCancel 处理 ClaimCancelExpired 已经改成 CANCELLED 的订单。
// DB 直写的库存已经在同一个 SQL 事务里加回去了。Redis 路径在这里跑还库存 Lua。
func FinishCancel(ctx context.Context, repo *order.Repo, st *stock.RedisStock, o model.Order) error {
	if o.ShardID < 0 || o.StockReturned {
		metrics.CancelTotal.WithLabelValues("db").Inc()
		return nil
	}
	if err := returnRedis(ctx, repo, st, o.CouponID, o.UserID, o.ShardID, o.ID); err != nil {
		metrics.CancelTotal.WithLabelValues("return_error").Inc()
		return err
	}
	metrics.CancelTotal.WithLabelValues("returned").Inc()
	return nil
}

// RepairReturns 重试那些已经取消、但 Redis 库存还没加回去的订单。
func RepairReturns(ctx context.Context, repo *order.Repo, st *stock.RedisStock, limit int) error {
	rows, err := repo.ListUnreturnedCancels(ctx, limit)
	if err != nil {
		return err
	}
	for _, o := range rows {
		if err := returnRedis(ctx, repo, st, o.CouponID, o.UserID, o.ShardID, o.ID); err != nil {
			return err
		}
		metrics.CancelTotal.WithLabelValues("repaired").Inc()
	}
	return nil
}

func returnRedis(ctx context.Context, repo *order.Repo, st *stock.RedisStock, couponID, userID int64, shard int, orderID int64) error {
	if _, err := st.Return(ctx, couponID, userID, shard); err != nil {
		return err
	}
	// Return 返回 false 表示用户已经不在集合里，库存之前就还过了。
	// 无论 true 还是 false，都可以把 stock_returned 标成 1，停掉修复循环。
	return repo.MarkStockReturned(ctx, orderID)
}

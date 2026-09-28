// Package reconcile 检查三条链路是否还能对上：
// Redis 剩余库存、MySQL 里仍占着库存的订单、还没落库的消息。
//
// 安静下来之后（没有未确认消息、没有“已取消但还没还库存”的行）应当满足：
//
//	Redis 剩余 + PENDING + CONFIRMED = 券的总库存
//	持有用户集合的大小 = PENDING + CONFIRMED
//	没有用户拥有两张仍有效的订单
//	没有任何分桶是负数
//
// 活动进行中这条等式可以暂时不成立：扣减已经发生，订单还在流里。
// 所以 Consistent 只在 Quiescent 时为 true。Oversell 则任何时候都不该大于 0。
package reconcile

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/victorzhong0110/teabreak/internal/model"
	"github.com/victorzhong0110/teabreak/internal/order"
	"github.com/victorzhong0110/teabreak/internal/stock"
)

type Snapshot struct {
	Strategy          model.Strategy
	Total             int
	RedisRemaining    int
	NegativeShards    int
	DBPending         int
	DBConfirmed       int
	DBCancelled       int
	DBRemaining       int
	DuplicateUsers    int
	RedisUsers        int
	MQPending         int
	MQLag             int
	DLQ               int
	UnreturnedCancels int
}

type Report struct {
	CouponID   int64    `json:"coupon_id"`
	Snapshot   Snapshot `json:"snapshot"`
	Active     int      `json:"active"`
	Quiescent  bool     `json:"quiescent"`
	Balanced   bool     `json:"balanced"`
	Oversell   int      `json:"oversell"`
	Consistent bool     `json:"consistent"`
	Notes      []string `json:"notes,omitempty"`
}

type Checker struct {
	repo   *order.Repo
	st     *stock.RedisStock
	rdb    redis.UniversalClient
	stream string
	dlq    string
	group  string
}

func New(repo *order.Repo, st *stock.RedisStock, rdb redis.UniversalClient, stream, dlq, group string) *Checker {
	return &Checker{repo: repo, st: st, rdb: rdb, stream: stream, dlq: dlq, group: group}
}

func (c *Checker) Check(ctx context.Context, couponID int64) (Report, error) {
	cp, err := c.repo.GetCoupon(ctx, couponID)
	if err != nil {
		return Report{}, err
	}
	if cp == nil {
		return Report{}, fmt.Errorf("coupon %d not found", couponID)
	}
	snap := Snapshot{
		Strategy:    cp.Strategy,
		Total:       cp.TotalStock,
		DBRemaining: cp.Remaining,
	}
	counts, err := c.repo.CountStatus(ctx, couponID)
	if err != nil {
		return Report{}, err
	}
	snap.DBPending = counts.Pending
	snap.DBConfirmed = counts.Confirmed
	snap.DBCancelled = counts.Cancelled
	dup, err := c.repo.CountDuplicateActiveUsers(ctx, couponID)
	if err != nil {
		return Report{}, err
	}
	snap.DuplicateUsers = dup
	unret, err := c.repo.CountUnreturned(ctx, couponID)
	if err != nil {
		return Report{}, err
	}
	snap.UnreturnedCancels = unret

	if cp.Strategy != model.StrategyDB {
		levels, err := c.st.Levels(ctx, couponID, cp.ShardCount)
		if err != nil {
			return Report{}, err
		}
		for _, n := range levels {
			if n < 0 {
				snap.NegativeShards++
				snap.RedisRemaining += n
				continue
			}
			snap.RedisRemaining += n
		}
		users, err := c.st.UserCount(ctx, couponID)
		if err != nil {
			return Report{}, err
		}
		snap.RedisUsers = int(users)
		pending, lag, err := groupLag(ctx, c.rdb, c.stream, c.group)
		if err != nil {
			return Report{}, err
		}
		snap.MQPending = pending
		snap.MQLag = lag
		dlq, err := c.rdb.XLen(ctx, c.dlq).Result()
		if err != nil && err != redis.Nil {
			return Report{}, err
		}
		snap.DLQ = int(dlq)
	}
	return Evaluate(couponID, snap), nil
}

func groupLag(ctx context.Context, rdb redis.UniversalClient, stream, group string) (pending, lag int, err error) {
	groups, err := rdb.XInfoGroups(ctx, stream).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, 0, nil
		}
		// 流还不存在时，go-redis 返回一条带 ERR 的错误。当成空流。
		if isNoGroup(err) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	for _, g := range groups {
		if g.Name == group {
			return int(g.Pending), int(g.Lag), nil
		}
	}
	// 流在，但这个消费组还没建。未消费条数就是整条流。
	n, err := rdb.XLen(ctx, stream).Result()
	if err != nil {
		return 0, 0, err
	}
	return 0, int(n), nil
}

func isNoGroup(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "no such key") ||
		strings.Contains(s, "NOGROUP") ||
		strings.Contains(s, "requires the key to exist")
}

// Evaluate 是纯函数，单测不需要数据库。
func Evaluate(couponID int64, s Snapshot) Report {
	active := s.DBPending + s.DBConfirmed
	rep := Report{
		CouponID: couponID,
		Snapshot: s,
		Active:   active,
		Notes:    nil,
	}
	if s.Strategy == model.StrategyDB {
		rep.Quiescent = s.UnreturnedCancels == 0
		rep.Balanced = s.DBRemaining+active == s.Total
		if active > s.Total {
			rep.Oversell = active - s.Total
		}
		if s.DBRemaining < 0 {
			rep.Oversell += -s.DBRemaining
		}
		rep.Consistent = rep.Quiescent && rep.Balanced && rep.Oversell == 0 && s.DuplicateUsers == 0
		if !rep.Balanced {
			rep.Notes = append(rep.Notes, fmt.Sprintf("db remaining %d + active %d != total %d", s.DBRemaining, active, s.Total))
		}
		return rep
	}

	// 一条流里有多张券。全局 lag 不是这张券的属性：别的券还在排队时，
	// 这张券只要 redis剩余 + 有效订单 = 总库存，就说明它自己没有在途扣减。
	// 在途扣减会让左边变小，Balanced 为 false。死信和未还库存仍然算未安静。
	rep.Quiescent = s.DLQ == 0 && s.UnreturnedCancels == 0
	rep.Balanced = s.RedisRemaining+active == s.Total && s.RedisUsers == active
	if active > s.Total {
		rep.Oversell = active - s.Total
	}
	if s.RedisRemaining < 0 {
		rep.Oversell += -s.RedisRemaining
	}
	if s.NegativeShards > 0 {
		rep.Oversell += s.NegativeShards
	}
	rep.Consistent = rep.Quiescent && rep.Balanced && rep.Oversell == 0 && s.DuplicateUsers == 0
	if s.MQPending != 0 || s.MQLag != 0 {
		rep.Notes = append(rep.Notes, fmt.Sprintf("共享流还有 pending=%d lag=%d（可能属于其他券）", s.MQPending, s.MQLag))
	}
	if !rep.Quiescent {
		rep.Notes = append(rep.Notes, "死信不为空，或有取消单的库存还没还回 Redis")
	}
	if s.RedisRemaining+active != s.Total {
		rep.Notes = append(rep.Notes, fmt.Sprintf("redis %d + active %d != total %d", s.RedisRemaining, active, s.Total))
	}
	if s.RedisUsers != active {
		rep.Notes = append(rep.Notes, fmt.Sprintf("redis users %d != active orders %d", s.RedisUsers, active))
	}
	if s.DuplicateUsers > 0 {
		rep.Notes = append(rep.Notes, fmt.Sprintf("duplicate active users %d", s.DuplicateUsers))
	}
	return rep
}

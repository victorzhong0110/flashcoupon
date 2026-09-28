// Package order 是 MySQL 访问层。
//
// 一人一单的数据库兜底不是 UNIQUE(user_id, coupon_id)：
// 用户超时取消之后应该还能再抢，旧的取消行要留着做审计。
// MySQL 没有部分唯一索引，所以用 active_user_id：
// 持有库存时它等于 user_id，取消时改成 NULL。唯一索引允许多个 NULL，
// 因此同一用户可以有很多张已取消的历史单，但只能有一张仍占库存的单。
package order

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/victorzhong0110/teabreak/internal/model"
)

//go:embed sql/001_init.sql
var migrationFS embed.FS

var ErrSoldOut = errors.New("sold_out")

type Repo struct {
	db *sql.DB
}

func Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(200)
	db.SetMaxIdleConns(50)
	db.SetConnMaxLifetime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func Migrate(db *sql.DB) error {
	b, err := migrationFS.ReadFile("sql/001_init.sql")
	if err != nil {
		return err
	}
	for _, stmt := range strings.Split(string(b), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("migrate: %w\n%s", err, stmt)
		}
	}
	return nil
}

func NewRepo(db *sql.DB) *Repo { return &Repo{db: db} }

func (r *Repo) CreateCoupon(ctx context.Context, c *model.Coupon) error {
	res, err := r.db.ExecContext(ctx, `
INSERT INTO coupons
  (name, total_stock, shard_count, remaining, strategy, start_at, end_at, pay_timeout_sec, per_user_limit)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.Name, c.TotalStock, c.ShardCount, c.TotalStock, string(c.Strategy),
		c.StartAt.UTC(), c.EndAt.UTC(), c.PayTimeoutSec, c.PerUserLimit,
	)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	c.ID = id
	c.Remaining = c.TotalStock
	return nil
}

func (r *Repo) GetCoupon(ctx context.Context, id int64) (*model.Coupon, error) {
	row := r.db.QueryRowContext(ctx, `
SELECT id, name, total_stock, shard_count, remaining, strategy, start_at, end_at, pay_timeout_sec, per_user_limit, created_at
FROM coupons WHERE id=?`, id)
	c, err := scanCoupon(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return c, err
}

func (r *Repo) ListCoupons(ctx context.Context) ([]model.Coupon, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT id, name, total_stock, shard_count, remaining, strategy, start_at, end_at, pay_timeout_sec, per_user_limit, created_at
FROM coupons ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Coupon
	for rows.Next() {
		c, err := scanCoupon(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

// GrabDB 是同步基线：一行券记录上的 remaining 用事务扣减，订单在同一个事务里插入。
// 失败回滚，库存不会少。慢的原因是所有请求都在给同一行加锁。
func (r *Repo) GrabDB(ctx context.Context, o *model.Order) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx,
		`UPDATE coupons SET remaining = remaining - 1 WHERE id=? AND remaining > 0`, o.CouponID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrSoldOut
	}
	if err := insertOrderTx(ctx, tx, o); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *Repo) InsertOrder(ctx context.Context, o *model.Order) error {
	return insertOrder(ctx, r.db, o)
}

func insertOrder(ctx context.Context, db execer, o *model.Order) error {
	return insertOrderTx(ctx, db, o)
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertOrderTx(ctx context.Context, db execer, o *model.Order) error {
	var active any
	if o.Status == model.StatusPending || o.Status == model.StatusConfirmed {
		active = o.UserID
	}
	_, err := db.ExecContext(ctx, `
INSERT INTO orders
  (id, coupon_id, user_id, idempotency_key, status, shard_id, active_user_id, stock_returned, created_at, expire_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		o.ID, o.CouponID, o.UserID, o.IdempotencyKey, string(o.Status), o.ShardID, active,
		boolToTiny(o.StockReturned), o.CreatedAt.UTC(), o.ExpireAt.UTC(), o.UpdatedAt.UTC(),
	)
	return err
}

func (r *Repo) GetOrder(ctx context.Context, id int64) (*model.Order, error) {
	row := r.db.QueryRowContext(ctx, orderSelect+` WHERE id=?`, id)
	o, err := scanOrder(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return o, err
}

func (r *Repo) GetOrderByIdem(ctx context.Context, couponID int64, key string) (*model.Order, error) {
	row := r.db.QueryRowContext(ctx, orderSelect+` WHERE coupon_id=? AND idempotency_key=?`, couponID, key)
	o, err := scanOrder(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return o, err
}

func (r *Repo) Confirm(ctx context.Context, id int64) (bool, error) {
	res, err := r.db.ExecContext(ctx, `
UPDATE orders SET status=?, updated_at=? WHERE id=? AND status=?`,
		string(model.StatusConfirmed), time.Now().UTC(), id, string(model.StatusPending))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// CancelIfPending 把仍在 PENDING 的订单改成 CANCELLED。
// DB 直写的单（shard_id < 0）在同一个事务里把 remaining 加回去。
// Redis 路径先只改状态，库存归还放到事务外的 Lua，并用 stock_returned 记录是否还过。
func (r *Repo) CancelIfPending(ctx context.Context, id int64) (*model.Order, bool, error) {
	o, err := r.GetOrder(ctx, id)
	if err != nil || o == nil {
		return o, false, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	returned := 0
	if o.ShardID < 0 {
		returned = 1
	}
	res, err := tx.ExecContext(ctx, `
UPDATE orders
SET status=?, active_user_id=NULL, stock_returned=?, updated_at=?
WHERE id=? AND status=?`,
		string(model.StatusCancelled), returned, time.Now().UTC(), id, string(model.StatusPending))
	if err != nil {
		return nil, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	if n == 0 {
		return o, false, nil
	}
	if o.ShardID < 0 {
		if _, err := tx.ExecContext(ctx,
			`UPDATE coupons SET remaining = remaining + 1 WHERE id=?`, o.CouponID); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	o.Status = model.StatusCancelled
	o.StockReturned = returned == 1
	return o, true, nil
}

func (r *Repo) MarkStockReturned(ctx context.Context, id int64) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE orders SET stock_returned=1, updated_at=? WHERE id=?`, time.Now().UTC(), id)
	return err
}

func (r *Repo) ListExpiredPending(ctx context.Context, couponID int64, limit int) ([]model.Order, error) {
	if limit <= 0 {
		limit = 100
	}
	// couponID>0 时只看这一张券。全局扫描按 expire_at 取最老的一批，
	// 库里积压几十万张过期单时，新超时的券要排很久才轮到。
	q := orderSelect + `
WHERE status=? AND expire_at <= UTC_TIMESTAMP(3)`
	args := []any{string(model.StatusPending)}
	if couponID > 0 {
		q += ` AND coupon_id=?`
		args = append(args, couponID)
	}
	q += `
ORDER BY expire_at
LIMIT ?`
	args = append(args, limit)
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectOrders(rows)
}

// ClaimCancelExpired 在一个事务里锁住一批已过期的 PENDING，改成 CANCELLED 后提交。
// freshWindow > 0 时只领 expire_at 落在「现在往前 freshWindow」里的单，
// 这样几十万张更老的积压不会挡住刚超时的订单。
// freshWindow == 0 时按 expire_at 从最老的开始清积压。
// 多个协程同时调时用 SKIP LOCKED，不会抢同一行。
// Redis 路径的库存不在这个事务里还，调用方拿到订单后再跑幂等 Lua。
func (r *Repo) ClaimCancelExpired(ctx context.Context, couponID int64, limit int, freshWindow time.Duration) ([]model.Order, error) {
	if limit <= 0 {
		limit = 100
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	q := orderSelect + `
WHERE status=? AND expire_at <= UTC_TIMESTAMP(3)`
	args := []any{string(model.StatusPending)}
	if couponID > 0 {
		q += ` AND coupon_id=?`
		args = append(args, couponID)
	}
	if freshWindow > 0 {
		q += ` AND expire_at >= UTC_TIMESTAMP(3) - INTERVAL ? MICROSECOND`
		args = append(args, freshWindow.Microseconds())
	}
	q += `
ORDER BY expire_at
LIMIT ?
FOR UPDATE SKIP LOCKED`
	args = append(args, limit)
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	claimed, err := collectOrders(rows)
	_ = rows.Close()
	if err != nil {
		return nil, err
	}
	if len(claimed) == 0 {
		return nil, nil
	}

	now := time.Now().UTC()
	kept := claimed[:0]
	for i := range claimed {
		o := &claimed[i]
		returned := 0
		if o.ShardID < 0 {
			returned = 1
		}
		res, err := tx.ExecContext(ctx, `
UPDATE orders
SET status=?, active_user_id=NULL, stock_returned=?, updated_at=?
WHERE id=? AND status=?`,
			string(model.StatusCancelled), returned, now, o.ID, string(model.StatusPending))
		if err != nil {
			return nil, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if n != 1 {
			continue
		}
		if o.ShardID < 0 {
			if _, err := tx.ExecContext(ctx,
				`UPDATE coupons SET remaining = remaining + 1 WHERE id=?`, o.CouponID); err != nil {
				return nil, err
			}
		}
		o.Status = model.StatusCancelled
		o.StockReturned = returned == 1
		o.UpdatedAt = now
		kept = append(kept, *o)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return kept, nil
}

func (r *Repo) ListUnreturnedCancels(ctx context.Context, limit int) ([]model.Order, error) {
	rows, err := r.db.QueryContext(ctx, orderSelect+`
WHERE status=? AND stock_returned=0 AND shard_id >= 0
LIMIT ?`, string(model.StatusCancelled), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectOrders(rows)
}

type StatusCount struct {
	Pending   int
	Confirmed int
	Cancelled int
}

func (r *Repo) CountStatus(ctx context.Context, couponID int64) (StatusCount, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT status, COUNT(*) FROM orders WHERE coupon_id=? GROUP BY status`, couponID)
	if err != nil {
		return StatusCount{}, err
	}
	defer rows.Close()
	var c StatusCount
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return StatusCount{}, err
		}
		switch model.OrderStatus(status) {
		case model.StatusPending:
			c.Pending = n
		case model.StatusConfirmed:
			c.Confirmed = n
		case model.StatusCancelled:
			c.Cancelled = n
		}
	}
	return c, rows.Err()
}

func (r *Repo) CountDuplicateActiveUsers(ctx context.Context, couponID int64) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM (
  SELECT user_id FROM orders
  WHERE coupon_id=? AND status IN (?, ?)
  GROUP BY user_id HAVING COUNT(*) > 1
) d`, couponID, string(model.StatusPending), string(model.StatusConfirmed)).Scan(&n)
	return n, err
}

func (r *Repo) ListActiveUserIDs(ctx context.Context, couponID int64) ([]int64, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT user_id FROM orders WHERE coupon_id=? AND status IN (?, ?)`,
		couponID, string(model.StatusPending), string(model.StatusConfirmed))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *Repo) CountUnreturned(ctx context.Context, couponID int64) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM orders
WHERE coupon_id=? AND status=? AND stock_returned=0 AND shard_id >= 0`,
		couponID, string(model.StatusCancelled)).Scan(&n)
	return n, err
}

func (r *Repo) DBRemaining(ctx context.Context, couponID int64) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `SELECT remaining FROM coupons WHERE id=?`, couponID).Scan(&n)
	return n, err
}

func IsDuplicate(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}

const orderSelect = `
SELECT id, coupon_id, user_id, idempotency_key, status, shard_id, stock_returned, created_at, expire_at, updated_at
FROM orders `

type scanner interface {
	Scan(dest ...any) error
}

func scanCoupon(s scanner) (*model.Coupon, error) {
	var c model.Coupon
	var strategy string
	if err := s.Scan(&c.ID, &c.Name, &c.TotalStock, &c.ShardCount, &c.Remaining, &strategy,
		&c.StartAt, &c.EndAt, &c.PayTimeoutSec, &c.PerUserLimit, &c.CreatedAt); err != nil {
		return nil, err
	}
	c.Strategy = model.Strategy(strategy)
	c.StartAt = c.StartAt.UTC()
	c.EndAt = c.EndAt.UTC()
	return &c, nil
}

func scanOrder(s scanner) (*model.Order, error) {
	var o model.Order
	var status string
	var returned int
	if err := s.Scan(&o.ID, &o.CouponID, &o.UserID, &o.IdempotencyKey, &status, &o.ShardID,
		&returned, &o.CreatedAt, &o.ExpireAt, &o.UpdatedAt); err != nil {
		return nil, err
	}
	o.Status = model.OrderStatus(status)
	o.StockReturned = returned == 1
	o.CreatedAt = o.CreatedAt.UTC()
	o.ExpireAt = o.ExpireAt.UTC()
	o.UpdatedAt = o.UpdatedAt.UTC()
	return &o, nil
}

func collectOrders(rows *sql.Rows) ([]model.Order, error) {
	var out []model.Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *o)
	}
	return out, rows.Err()
}

func boolToTiny(b bool) int {
	if b {
		return 1
	}
	return 0
}

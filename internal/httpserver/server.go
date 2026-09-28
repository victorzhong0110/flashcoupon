// Package httpserver 把抢券服务暴露成 JSON API。
// 用户身份在这里用请求头 X-User-Id 代替登录态。这是演示边界：
// 生产环境必须换成校验过的会话，否则谁都可以冒充别人的 user id。
package httpserver

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/victorzhong0110/teabreak/internal/model"
	"github.com/victorzhong0110/teabreak/internal/reconcile"
	"github.com/victorzhong0110/teabreak/internal/seckill"
)

type Server struct {
	svc   *seckill.Service
	check *reconcile.Checker
	log   *slog.Logger
}

func New(svc *seckill.Service, check *reconcile.Checker, log *slog.Logger, _ bool) *gin.Engine {
	if log == nil {
		log = slog.Default()
	}
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	s := &Server{svc: svc, check: check, log: log}
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	r.POST("/v1/coupons", s.create)
	r.GET("/v1/coupons", s.list)
	r.GET("/v1/coupons/:id", s.get)
	r.GET("/v1/coupons/:id/stock", s.stock)
	r.GET("/v1/coupons/:id/reconcile", s.reconcile)
	r.POST("/v1/coupons/:id/grab", s.grab)
	r.GET("/v1/orders/:id", s.getOrder)
	r.POST("/v1/orders/:id/confirm", s.confirm)
	r.POST("/v1/orders/:id/cancel", s.cancel)
	return r
}

type createBody struct {
	Name          string     `json:"name"`
	TotalStock    int        `json:"total_stock"`
	ShardCount    int        `json:"shard_count"`
	Strategy      string     `json:"strategy"`
	StartAt       *time.Time `json:"start_at"`
	EndAt         *time.Time `json:"end_at"`
	PayTimeoutSec int        `json:"pay_timeout_sec"`
}

func (s *Server) create(c *gin.Context) {
	var body createBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad_request", "message": err.Error()})
		return
	}
	now := time.Now()
	start := now.Add(-time.Second)
	end := now.Add(24 * time.Hour)
	if body.StartAt != nil {
		start = *body.StartAt
	}
	if body.EndAt != nil {
		end = *body.EndAt
	}
	strategy := model.Strategy(body.Strategy)
	if strategy == "" {
		strategy = model.StrategySharded
	}
	shards := body.ShardCount
	if shards == 0 && strategy == model.StrategySharded {
		shards = 8
	}
	if shards == 0 {
		shards = 1
	}
	cp, err := s.svc.CreateCoupon(c.Request.Context(), seckill.CreateInput{
		Name: body.Name, TotalStock: body.TotalStock, ShardCount: shards,
		Strategy: strategy, StartAt: start, EndAt: end, PayTimeoutSec: body.PayTimeoutSec,
	})
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusCreated, couponJSON(cp))
}

func (s *Server) list(c *gin.Context) {
	rows, err := s.svc.ListCoupons(c.Request.Context())
	if err != nil {
		writeErr(c, err)
		return
	}
	out := make([]gin.H, 0, len(rows))
	for i := range rows {
		out = append(out, couponJSON(&rows[i]))
	}
	c.JSON(http.StatusOK, gin.H{"coupons": out})
}

func (s *Server) get(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	cp, err := s.svc.GetCoupon(c.Request.Context(), id)
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, couponJSON(cp))
}

func (s *Server) stock(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	levels, err := s.svc.StockLevels(c.Request.Context(), id)
	if err != nil {
		writeErr(c, err)
		return
	}
	sum := 0
	for _, n := range levels {
		sum += n
	}
	c.JSON(http.StatusOK, gin.H{"coupon_id": id, "shards": levels, "remaining": sum})
}

func (s *Server) reconcile(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	rep, err := s.check.Check(c.Request.Context(), id)
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, rep)
}

func (s *Server) grab(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	userID, err := strconv.ParseInt(c.GetHeader("X-User-Id"), 10, 64)
	if err != nil || userID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad_request", "message": "需要正整数请求头 X-User-Id"})
		return
	}
	idem := c.GetHeader("Idempotency-Key")
	res, err := s.svc.Grab(c.Request.Context(), userID, id, idem)
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, res)
}

func (s *Server) getOrder(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	o, err := s.svc.Repo().GetOrder(c.Request.Context(), id)
	if err != nil {
		writeErr(c, err)
		return
	}
	if o == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "not_found", "message": "订单不存在"})
		return
	}
	c.JSON(http.StatusOK, orderJSON(o))
}

func (s *Server) confirm(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	o, err := s.svc.Confirm(c.Request.Context(), id)
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, orderJSON(o))
}

func (s *Server) cancel(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	o, err := s.svc.Cancel(c.Request.Context(), id)
	if err != nil {
		writeErr(c, err)
		return
	}
	c.JSON(http.StatusOK, orderJSON(o))
}

func pathID(c *gin.Context) (int64, bool) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad_request", "message": "非法 id"})
		return 0, false
	}
	return id, true
}

func writeErr(c *gin.Context, err error) {
	switch {
	case errors.Is(err, seckill.ErrSoldOut):
		c.JSON(http.StatusConflict, gin.H{"error": "sold_out", "message": "已经抢光了"})
	case errors.Is(err, seckill.ErrAlready):
		c.JSON(http.StatusConflict, gin.H{"error": "already_owned", "message": "你已经抢到过这张券"})
	case errors.Is(err, seckill.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "not_found", "message": "券不存在"})
	case errors.Is(err, seckill.ErrLimited):
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate_limited", "message": "请求太频繁，请稍后再试"})
	case errors.Is(err, seckill.ErrNotStarted):
		c.JSON(http.StatusForbidden, gin.H{"error": "not_started", "message": "活动还没开始"})
	case errors.Is(err, seckill.ErrEnded):
		c.JSON(http.StatusForbidden, gin.H{"error": "ended", "message": "活动已经结束"})
	case errors.Is(err, seckill.ErrBadRequest):
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad_request", "message": err.Error()})
	case errors.Is(err, seckill.ErrNotPending):
		c.JSON(http.StatusConflict, gin.H{"error": "not_pending", "message": "订单不在待确认状态"})
	case errors.Is(err, seckill.ErrUnavailable):
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "unavailable", "message": "依赖暂时不可用"})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal", "message": "服务器内部错误"})
	}
}

func couponJSON(c *model.Coupon) gin.H {
	return gin.H{
		"id":              c.ID,
		"name":            c.Name,
		"total_stock":     c.TotalStock,
		"shard_count":     c.ShardCount,
		"strategy":        c.Strategy,
		"start_at":        c.StartAt.UTC().Format(time.RFC3339Nano),
		"end_at":          c.EndAt.UTC().Format(time.RFC3339Nano),
		"pay_timeout_sec": c.PayTimeoutSec,
		"per_user_limit":  c.PerUserLimit,
		// remaining 只对 strategy=db 有意义。Redis 路径请看 /stock。
		"db_remaining": c.Remaining,
	}
}

func orderJSON(o *model.Order) gin.H {
	return gin.H{
		"id":              o.ID,
		"coupon_id":       o.CouponID,
		"user_id":         o.UserID,
		"idempotency_key": o.IdempotencyKey,
		"status":          o.Status,
		"shard_id":        o.ShardID,
		"stock_returned":  o.StockReturned,
		"created_at":      o.CreatedAt.UTC().Format(time.RFC3339Nano),
		"expire_at":       o.ExpireAt.UTC().Format(time.RFC3339Nano),
	}
}

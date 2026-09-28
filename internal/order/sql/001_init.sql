-- 订单表故意不加指向 coupons 的外键。
-- 外键插入时要回表检查父行，高并发下会把券那一行又变成热点，
-- Redis 预扣想躲开的行锁就从后门回来了。完整性靠对账脚本看。

CREATE TABLE IF NOT EXISTS coupons (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(128) NOT NULL,
  total_stock INT NOT NULL,
  shard_count INT NOT NULL,
  remaining INT NOT NULL,
  strategy VARCHAR(16) NOT NULL,
  start_at DATETIME(3) NOT NULL,
  end_at DATETIME(3) NOT NULL,
  pay_timeout_sec INT NOT NULL,
  per_user_limit INT NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS orders (
  id BIGINT PRIMARY KEY,
  coupon_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  idempotency_key VARCHAR(80) NOT NULL,
  status VARCHAR(16) NOT NULL,
  shard_id INT NOT NULL,
  active_user_id BIGINT NULL,
  stock_returned TINYINT NOT NULL DEFAULT 0,
  created_at DATETIME(3) NOT NULL,
  expire_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  UNIQUE KEY uk_idem (coupon_id, idempotency_key),
  UNIQUE KEY uk_active_user (coupon_id, active_user_id),
  KEY idx_coupon_status (coupon_id, status),
  KEY idx_status_expire (status, expire_at),
  KEY idx_cancel_repair (status, stock_returned)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

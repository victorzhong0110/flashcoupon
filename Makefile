.PHONY: test test-race test-integration up down bench

export MYSQL_DSN ?= root:root@tcp(127.0.0.1:13306)/coupon?parseTime=true&charset=utf8mb4&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s
export REDIS_ADDR ?= 127.0.0.1:16379

test:
	go test -count=1 ./...

test-race:
	go test -race -count=1 ./...

test-integration:
	go test -race -count=1 ./internal/integration/

up:
	docker compose up -d --build

down:
	docker compose down

bench:
	bash scripts/bench.sh

bench-realistic:
	bash scripts/bench-realistic.sh

package stock

import "testing"

func TestPlan(t *testing.T) {
	src, dst, amount, ok := Plan([]int{10, 0, 1}, 3)
	if !ok || src != 0 || dst != 1 || amount != 5 {
		t.Fatalf("got %d %d %d %v", src, dst, amount, ok)
	}
	if _, _, _, ok := Plan([]int{5, 5, 5}, 2); ok {
		t.Fatal("balanced stock must not move")
	}
	// 差 1 时不搬，否则两个桶会来回对调。
	if _, _, _, ok := Plan([]int{1, 0}, 0); ok {
		t.Fatal("gap of 1 must not oscillate")
	}
	if _, _, _, ok := Plan([]int{4}, 0); ok {
		t.Fatal("one shard")
	}
}

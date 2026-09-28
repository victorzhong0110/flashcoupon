package stock

import "testing"

func TestSplitSumsToTotal(t *testing.T) {
	cases := []struct{ total, n int }{
		{0, 4},
		{1, 4},
		{7, 4},
		{8, 4},
		{100, 8},
		{1000, 1},
		{5, 8},
	}
	for _, tc := range cases {
		parts := Split(tc.total, tc.n)
		if len(parts) != tc.n {
			t.Fatalf("len %d", len(parts))
		}
		sum := 0
		for _, p := range parts {
			if p < 0 {
				t.Fatalf("negative part %d", p)
			}
			sum += p
		}
		if sum != tc.total {
			t.Fatalf("total %d n %d sum %d parts %v", tc.total, tc.n, sum, parts)
		}
	}
}

func TestPreferShardStable(t *testing.T) {
	a := PreferShard(42, 8)
	b := PreferShard(42, 8)
	if a != b || a < 0 || a >= 8 {
		t.Fatalf("shard %d then %d", a, b)
	}
	if PreferShard(1, 1) != 0 {
		t.Fatal("single shard")
	}
}

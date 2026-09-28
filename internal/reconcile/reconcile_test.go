package reconcile

import (
	"testing"

	"github.com/victorzhong0110/teabreak/internal/model"
)

func TestEvaluateQuiescentMatch(t *testing.T) {
	rep := Evaluate(1, Snapshot{
		Strategy:       model.StrategySharded,
		Total:          100,
		RedisRemaining: 40,
		DBPending:      50,
		DBConfirmed:    10,
		RedisUsers:     60,
	})
	if !rep.Consistent || rep.Oversell != 0 {
		t.Fatalf("%+v", rep)
	}
}

func TestEvaluateInFlightIsNotYetConsistent(t *testing.T) {
	// 扣了 10 个还在流里，DB 只有 50。安静条件不满足，但不能算超卖。
	rep := Evaluate(1, Snapshot{
		Strategy:       model.StrategyRedis,
		Total:          100,
		RedisRemaining: 40,
		DBPending:      50,
		RedisUsers:     60,
		MQLag:          10,
	})
	if rep.Consistent {
		t.Fatal("in-flight traffic must not be reported consistent")
	}
	if rep.Oversell != 0 {
		t.Fatalf("oversell %d", rep.Oversell)
	}
}

func TestEvaluateOversell(t *testing.T) {
	rep := Evaluate(1, Snapshot{
		Strategy:    model.StrategyDB,
		Total:       10,
		DBRemaining: 0,
		DBPending:   12,
	})
	if rep.Oversell != 2 || rep.Consistent {
		t.Fatalf("%+v", rep)
	}
}

func TestEvaluateDuplicateUsers(t *testing.T) {
	rep := Evaluate(1, Snapshot{
		Strategy:       model.StrategySharded,
		Total:          10,
		RedisRemaining: 9,
		DBPending:      1,
		RedisUsers:     1,
		DuplicateUsers: 1,
	})
	if rep.Consistent {
		t.Fatal("duplicates must fail the check")
	}
}

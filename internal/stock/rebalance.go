package stock

// Plan 决定要不要把库存从最满的桶搬到最空的桶。
//
// 只搬差额的一半，避免下一轮把货再搬回去（振荡）。
// 差额不足 2 时一半是 0，直接放弃：搬 1 个会让两个桶对调，下一秒再对调回来。
func Plan(levels []int, threshold int) (src, dst, amount int, ok bool) {
	if len(levels) < 2 {
		return 0, 0, 0, false
	}
	maxI, minI := 0, 0
	for i, v := range levels {
		if v > levels[maxI] {
			maxI = i
		}
		if v < levels[minI] {
			minI = i
		}
	}
	if maxI == minI {
		return 0, 0, 0, false
	}
	gap := levels[maxI] - levels[minI]
	if gap <= threshold {
		return 0, 0, 0, false
	}
	amount = gap / 2
	if amount < 1 {
		return 0, 0, 0, false
	}
	return maxI, minI, amount, true
}

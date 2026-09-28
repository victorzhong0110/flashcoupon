package stock

import (
	"encoding/binary"
	"hash/fnv"
)

// Split 把总量摊到 n 个桶。前 total%n 个桶多 1，保证求和严格等于 total。
// 不能用“每个桶四舍五入”，否则总和会漂，对账公式直接坏掉。
func Split(total, n int) []int {
	if n <= 0 {
		return nil
	}
	if total < 0 {
		total = 0
	}
	out := make([]int, n)
	base := total / n
	rem := total % n
	for i := 0; i < n; i++ {
		out[i] = base
		if i < rem {
			out[i]++
		}
	}
	return out
}

// PreferShard 把用户稳定地映射到一个桶。
// 同一个人每次都先打同一个桶，分桶才不会把“一人一单”的判断打散到随机 key 上。
// 这个桶空了，Lua 仍会继续看后面的桶，所以映射不均匀不会直接造成少卖。
func PreferShard(userID int64, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(userID))
	_, _ = h.Write(buf[:])
	return int(h.Sum32() % uint32(n))
}

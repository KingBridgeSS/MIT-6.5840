package kvraft

import (
	"strconv"
)

func Int64toInt(n int64) int {
	strInt64 := strconv.FormatInt(n, 10)
	id16, _ := strconv.Atoi(strInt64)
	return id16
}

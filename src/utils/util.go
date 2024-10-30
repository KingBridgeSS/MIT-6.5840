package utils

import (
	"math/rand"
	"strconv"
	"time"
	"unsafe"
)

func AddressToInt(ptr interface{}) int {
	// 使用 unsafe.Pointer 将地址转换为 uintptr，然后再转换为 int
	return int(uintptr(unsafe.Pointer(&ptr)))
}
func GenerateRandomNumber() string {
	// 使用当前时间的纳秒作为随机种子
	rand.Seed(time.Now().UnixNano())

	// 生成 100 到 999 之间的随机数
	randomNumber := rand.Intn(900) + 100

	// 将数字转换为字符串
	return strconv.Itoa(randomNumber)
}

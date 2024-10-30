package shardkv

import "log"

// const SDebug = true
// const CDebug = true

const SDebug = false
const CDebug = false

func (kv *ShardKV) SSPrintf(format string, a ...interface{}) (n int, err error) {
	if SDebug {
		log.Printf("[server %d]"+format, append([]interface{}{kv.gid}, a...)...)
	}
	return
}

func CCPrintf(format string, a ...interface{}) (n int, err error) {
	if CDebug {
		log.Printf("[client]"+format, a...)
	}
	return
}

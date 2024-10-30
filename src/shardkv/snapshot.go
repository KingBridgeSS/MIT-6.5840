package shardkv

import (
	"bytes"

	"6.5840/labgob"
	"6.5840/shardctrler"
)

func (kv *ShardKV) threshold() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	ans := kv.persister.RaftStateSize() > kv.maxraftstate
	// kv.SSPrintf("maxraftstate=%d,cur=%d", kv.maxraftstate, kv.persister.RaftStateSize())
	return ans
}

func (kv *ShardKV) takeSnapshot(lastIndex int) {
	// kv.SSPrintf("taking snapshot")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.shardMap)
	e.Encode(kv.seqMap)
	e.Encode(kv.curConfig)
	e.Encode(kv.prevConfig)
	kv.rf.Snapshot(lastIndex, w.Bytes())
}

func (kv *ShardKV) applySnapshot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var shardMap []Shard
	var seqMap map[int64]int
	var prevConfig, curConfig shardctrler.Config
	if d.Decode(&shardMap) != nil || d.Decode(&seqMap) != nil || d.Decode(&curConfig) != nil || d.Decode(&prevConfig) != nil {
		kv.SSPrintf("decoding %+v", data)
		panic("decode error")
	}
	kv.shardMap = shardMap
	kv.seqMap = seqMap
	kv.curConfig = curConfig
	kv.prevConfig = prevConfig
}

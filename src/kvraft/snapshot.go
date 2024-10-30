package kvraft

import (
	"bytes"

	"6.5840/labgob"
)

func (kv *KVServer) threshold() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	return kv.persister.RaftStateSize() > kv.maxraftstate
}

func (kv *KVServer) takeSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.lastApplied)
	e.Encode(kv.mp)
	e.Encode(kv.seqMap)

	kv.rf.Snapshot(kv.lastApplied, w.Bytes())
}

func (kv *KVServer) applySnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var lastApplied int
	var mp map[string]string
	var seqMap map[int64]int
	if d.Decode(&lastApplied) != nil || d.Decode(&mp) != nil || d.Decode(&seqMap) != nil {
		SPrintf("decode error")
	}
	SPrintf("server %d applying snapshot by index=%d", kv.me, lastApplied)

	kv.lastApplied = lastApplied
	kv.mp = mp
	kv.seqMap = seqMap
}

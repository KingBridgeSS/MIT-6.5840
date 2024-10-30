package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu   sync.Mutex
	mp   map[string]string
	done map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.done[args.RequestID]
	if ok {
		reply.Value = kv.done[args.RequestID]
	} else {
		v := kv.mp[args.Key]
		reply.Value = v
		kv.done[args.RequestID] = v
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.done[args.RequestID]
	if ok {
		reply.Value = kv.done[args.RequestID]
	} else {
		kv.mp[args.Key] = args.Value
		kv.done[args.RequestID] = ""
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.done[args.RequestID]
	if ok {
		reply.Value = kv.done[args.RequestID]
	} else {
		old := kv.mp[args.Key]
		kv.mp[args.Key] += args.Value
		reply.Value = old
		kv.done[args.RequestID] = old
	}
}
func (kv *KVServer) Resolve(args *ResolveArgs, reply *ResolveReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.done, args.RequestID)
}
func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.mp = make(map[string]string)
	kv.done = make(map[int64]string)
	return kv
}

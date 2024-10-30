package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const SDebug = false
const CDebug = false

func SPrintf(format string, a ...interface{}) (n int, err error) {
	if SDebug {
		DPrintf("[server]"+format, a...)
	}
	return
}
func CPrintf(format string, a ...interface{}) (n int, err error) {
	if CDebug {
		DPrintf("[client]"+format, a...)
	}
	return
}
func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.Printf(format, a...)
	return
}

const (
	GETOP    = "GET"
	PUTOP    = "PUT"
	APPENDOP = "APPEND"
)

const CHANNEL_TIMEOUT = 3000 // ms

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   string
	OpKey    string
	OpValue  string
	Seq      int
	ClientID int64
}
type opResult struct {
	err string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mp          map[string]string
	opResultCh  chan opResult
	resultChMap map[int]chan opResult
	lastApplied int
	seqMap      map[int64]int
	persister   *raft.Persister
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.opResultCh = make(chan opResult)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mp = make(map[string]string)
	kv.lastApplied = 0
	kv.resultChMap = make(map[int]chan opResult)
	kv.seqMap = make(map[int64]int)
	kv.persister = persister
	kv.applySnapshot(persister.ReadSnapshot())
	go kv.applier()
	return kv
}

func (kv *KVServer) getResultCh(lastIndex int) chan opResult {
	ch, ok := kv.resultChMap[lastIndex]
	if !ok {
		ch = make(chan opResult)
		kv.resultChMap[lastIndex] = ch
	}
	return ch
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		OpType:   GETOP,
		OpKey:    args.Key,
		Seq:      args.Seq,
		ClientID: args.ClientID,
	}
	lastIndex, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = NOTALEADER
		return
	}

	kv.mu.Lock()
	SPrintf("leader %d start index %d", kv.me, lastIndex)

	ch := kv.getResultCh(lastIndex)

	defer func() {
		kv.mu.Lock()
		delete(kv.resultChMap, lastIndex)
		kv.mu.Unlock()
	}()
	kv.mu.Unlock()
	ticker := time.NewTicker(time.Duration(CHANNEL_TIMEOUT) * time.Millisecond)
	defer ticker.Stop()
	select {
	case opResult := <-ch:
		if opResult.err != "" {
			reply.Err = Err(opResult.err)
		} else {
			kv.mu.Lock()
			reply.Value = kv.mp[args.Key]
			kv.mu.Unlock()
		}
	case <-ticker.C:
		reply.Err = NOTALEADER
	}

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		OpType:   PUTOP,
		OpKey:    args.Key,
		OpValue:  args.Value,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	lastIndex, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = NOTALEADER
		return
	}
	SPrintf("leader %d start index %d", kv.me, lastIndex)

	kv.mu.Lock()
	ch := kv.getResultCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.resultChMap, lastIndex)
		kv.mu.Unlock()
	}()
	kv.mu.Unlock()

	ticker := time.NewTicker(time.Duration(CHANNEL_TIMEOUT) * time.Millisecond)
	defer ticker.Stop()
	select {
	case opResult := <-ch:
		if opResult.err != "" {
			reply.Err = Err(opResult.err)
		}
	case <-ticker.C:
		// SPrintf("server timeout")
		reply.Err = NOTALEADER
	}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		OpType:   APPENDOP,
		OpKey:    args.Key,
		OpValue:  args.Value,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	lastIndex, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = NOTALEADER
		return
	}

	kv.mu.Lock()
	SPrintf("leader %d start index %d", kv.me, lastIndex)

	ch := kv.getResultCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.resultChMap, lastIndex)
		kv.mu.Unlock()
	}()
	kv.mu.Unlock()
	ticker := time.NewTicker(time.Duration(CHANNEL_TIMEOUT) * time.Millisecond)
	defer ticker.Stop()

	select {
	case opResult := <-ch:
		if opResult.err != "" {
			reply.Err = Err(opResult.err)
		}
	case <-ticker.C:
		reply.Err = NOTALEADER
	}
}
func (kv *KVServer) processSeq(op Op) bool {
	seq0, ok := kv.seqMap[op.ClientID]
	if !ok {
		kv.seqMap[op.ClientID] = op.Seq
		return true
	}
	if !(seq0+1 == op.Seq) {
		return false
	}
	kv.seqMap[op.ClientID]++
	return true
}
func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		_, isLeader := kv.rf.GetState()
		SPrintf("server %d recv:%+v", kv.me, msg)
		kv.mu.Lock()
		if msg.CommandValid && msg.CommandIndex > kv.lastApplied {
			op := msg.Command.(Op)
			// 符合seq增长规律才会write
			if kv.processSeq(op) {
				k := op.OpKey
				switch op.OpType {
				case PUTOP:
					{
						kv.mp[k] = op.OpValue
					}
				case APPENDOP:
					{
						kv.mp[k] = kv.mp[k] + op.OpValue
					}
				}
			}

			// ch := kv.getResultCh(msg.CommandIndex)
			ch, ok := kv.resultChMap[msg.CommandIndex]
			if ok {
				if isLeader {
					ch <- opResult{}
					SPrintf("leader %d applied index=%d", kv.me, msg.CommandIndex)
				} else {
					// SPrintf("follower %d stuck", kv.me)
					ch <- opResult{err: NOTALEADER}
				}
			}
			kv.lastApplied = msg.CommandIndex
			// DPrintf("server %d last applied update to %d", kv.me, msg.CommandIndex)
			if kv.threshold() {
				kv.takeSnapshot()
			}
		} else if msg.SnapshotValid {
			kv.applySnapshot(msg.Snapshot)
		} else {
			SPrintf("server %d denied", kv.me)
		}
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

package shardctrler

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const CHANNEL_TIMEOUT = 3000 // ms

const SDebug = true

func SPrintf(format string, a ...interface{}) (n int, err error) {
	if SDebug {
		DPrintf("[server]"+format, a...)
	}
	return
}

func (sc *ShardCtrler) SSPrintf(format string, a ...interface{}) (n int, err error) {
	if SDebug {
		log.Printf("[server%d]"+format, append([]interface{}{sc.me}, a...)...)
	}
	return
}

type opResult struct {
	err bool
}
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32
	// Your data here.

	configs     []Config // indexed by config num
	opResultCh  chan opResult
	resultChMap map[int]chan opResult
	lastApplied int
	seqMap      map[int64]int
	persister   *raft.Persister
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.opResultCh = make(chan opResult)
	sc.lastApplied = 0
	sc.resultChMap = make(map[int]chan opResult)
	sc.seqMap = make(map[int64]int)
	sc.persister = persister
	// The very first configuration should be numbered zero. It should contain no groups,
	// and all shards should be assigned to GID zero (an invalid GID).
	sc.configs = []Config{{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}}
	// TODO: snapshot
	go sc.applier()
	return sc
}

const JOIN = 0
const LEAVE = 1
const MOVE = 2
const QUERY = 3

type Op struct {
	OpType   int
	Seq      int
	ClientID int64
	Servers  map[int][]string
	GIDs     []int
	Shard    int
	GID      int
	Num      int
}

func (sc *ShardCtrler) getResultCh(lastIndex int) chan opResult {
	ch, ok := sc.resultChMap[lastIndex]
	if !ok {
		ch = make(chan opResult)
		sc.resultChMap[lastIndex] = ch
	}
	return ch
}
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		OpType:   JOIN,
		Seq:      args.Seq,
		ClientID: args.ClientID,
		Servers:  args.Servers,
	}
	lastIndex, _, ok := sc.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	ch := sc.getResultCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.resultChMap, lastIndex)
		sc.mu.Unlock()
	}()
	sc.mu.Unlock()
	ticker := getTimeoutTicker()
	defer ticker.Stop()
	select {
	case opResult := <-ch:
		if opResult.err {
			reply.WrongLeader = true
		}
	case <-ticker.C:
		reply.WrongLeader = true
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		OpType:   LEAVE,
		Seq:      args.Seq,
		ClientID: args.ClientID,
		GIDs:     args.GIDs,
	}
	lastIndex, _, ok := sc.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	ch := sc.getResultCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.resultChMap, lastIndex)
		sc.mu.Unlock()
	}()
	sc.mu.Unlock()
	ticker := getTimeoutTicker()
	defer ticker.Stop()
	select {
	case opResult := <-ch:
		if opResult.err {
			reply.WrongLeader = true
		}
	case <-ticker.C:
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		OpType:   MOVE,
		Seq:      args.Seq,
		ClientID: args.ClientID,
		Shard:    args.Shard,
		GID:      args.GID,
	}
	lastIndex, _, ok := sc.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	ch := sc.getResultCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.resultChMap, lastIndex)
		sc.mu.Unlock()
	}()
	sc.mu.Unlock()
	ticker := getTimeoutTicker()
	defer ticker.Stop()
	select {
	case opResult := <-ch:
		if opResult.err {
			reply.WrongLeader = true
		}
	case <-ticker.C:
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		OpType:   QUERY,
		Seq:      args.Seq,
		ClientID: args.ClientID,
		Num:      args.Num,
	}
	lastIndex, _, ok := sc.rf.Start(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	ch := sc.getResultCh(lastIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.resultChMap, lastIndex)
		sc.mu.Unlock()
	}()
	sc.mu.Unlock()
	ticker := getTimeoutTicker()
	defer ticker.Stop()
	select {
	case opResult := <-ch:
		if opResult.err {
			reply.WrongLeader = true
		} else {
			sc.mu.Lock()
			lastConfigIndex := len(sc.configs) - 1
			// SPrintf("Num=%d, all config: %+v", lastConfigIndex, sc.configs)
			if args.Num > lastConfigIndex || args.Num < 0 {
				reply.Config = sc.configs[lastConfigIndex]
			} else {
				reply.Config = sc.configs[args.Num]
			}
			sc.mu.Unlock()
		}
	case <-ticker.C:
		reply.WrongLeader = true
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func getTimeoutTicker() time.Ticker {
	ticker := time.NewTicker(time.Duration(CHANNEL_TIMEOUT) * time.Millisecond)
	return *ticker
}

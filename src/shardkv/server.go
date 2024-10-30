package shardkv

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const CHANNEL_TIMEOUT = 2000

type opResult struct {
	Err       string
	ConfigNum int
	Gid       int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	shardMap    []Shard               //ShardId -> Shard, wrong group if value == null
	resultChMap map[int]chan opResult // (raft) log index -> opResult
	seqMap      map[int64]int
	persister   *raft.Persister
	curConfig   shardctrler.Config
	prevConfig  shardctrler.Config
	mck         *shardctrler.Clerk
	dead        int32
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(RaftCommand{})
	labgob.Register(Op{})
	labgob.Register(PushShardInfo{})
	labgob.Register(UpdateConfigInfo{})
	labgob.Register(GarbageInfo{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.resultChMap = make(map[int]chan opResult)
	kv.seqMap = make(map[int64]int)
	kv.persister = persister
	kv.shardMap = make([]Shard, shardctrler.NShards)
	kv.applySnapshot(persister.ReadSnapshot())
	// kv.curConfig = kv.mck.Query(-1) // 初始化config
	go kv.applier()
	go kv.ConfigLoop()
	return kv
}
func (kv *ShardKV) getResultCh(lastIndex int) chan opResult {
	ch, ok := kv.resultChMap[lastIndex]
	if !ok {
		ch = make(chan opResult, 1)
		// kv.SSPrintf("new raft index %d", lastIndex)
		kv.resultChMap[lastIndex] = ch
	}
	return ch
}
func (kv *ShardKV) startCommand(cmd RaftCommand) Err {
	kv.mu.Lock()
	i, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	ch := kv.getResultCh(i)
	defer func() {
		kv.mu.Lock()
		// kv.SSPrintf("delete index=%d", i)
		delete(kv.resultChMap, i)
		kv.mu.Unlock()
	}()
	kv.mu.Unlock()
	timer := time.NewTicker(CHANNEL_TIMEOUT * time.Millisecond)
	defer timer.Stop()
	select {
	case r := <-ch:
		kv.mu.Lock()
		// delete(kv.resultChMap, i)
		if r.ConfigNum != cmd.ConfigNum || r.Gid != cmd.Gid {
			kv.mu.Unlock()
			return ErrInconsistent
		}
		kv.mu.Unlock()
		// kv.SSPrintf("raft returned %+v for cmd=%+v", r, cmd)
		return Err(r.Err)
	case <-timer.C:
		// kv.SSPrintf("timeout")
		return ErrTimeout
	}
}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	configNum := kv.curConfig.Num
	if kv.curConfig.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardMap[shardId].Mp == nil {
		// kv.SSPrintf("not ready for getting %s, shardId=%d", args.Key, shardId)
		reply.Err = ErrNotReady
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ErrNotReady {
		return
	}
	op := Op{
		OpType:   GETOP,
		OpKey:    args.Key,
		Seq:      args.Seq,
		ClientID: args.ClientID,
	}
	// kv.SSPrintf("getting sid=%d %+v", shardId, args)
	cmd := newRaftCommand(Operation, configNum, kv.gid, op)
	err := kv.startCommand(cmd)
	if err != OK {
		reply.Err = err
		return
	}
	kv.mu.Lock()
	// if kv.curConfig.Shards[shardId] != kv.gid {
	// 	reply.Err = ErrWrongGroup
	// } else if kv.shardMap[shardId].Mp == nil {
	// 	reply.Err = ErrNotReady
	// } else {
	reply.Err = OK
	reply.Value = kv.shardMap[shardId].Mp[args.Key]
	// }
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	n := kv.curConfig.Num
	if kv.curConfig.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardMap[shardId].Mp == nil {
		kv.SSPrintf("not ready for putting/appending %s, shardId=%d", args.Key, shardId)
		reply.Err = ErrNotReady
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ErrNotReady {
		return
	}
	var t OpType
	if args.Op == "Put" {
		t = PUTOP
	} else if args.Op == "Append" {
		t = APPENDOP
	}
	op := Op{
		OpType:   t,
		ClientID: args.ClientID,
		Seq:      args.Seq,
		OpKey:    args.Key,
		OpValue:  args.Value,
	}
	reply.Err = kv.startCommand(newRaftCommand(Operation, n, kv.gid, op))
	if reply.Err != ErrWrongLeader {
		kv.SSPrintf("making %s sid=%d, %+v,Err=%s", args.Op, shardId, args, reply.Err)
	}
}
func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	cmd := newRaftCommand(PushShard, args.ConfigNum, args.ClientGid, PushShardInfo{
		Shard:  args.Shard,
		SeqMap: args.SeqMap,
	})
	reply.Err = kv.startCommand(cmd)
	if reply.Err == OK {
		kv.SSPrintf("PushShardRPC: cmd=%+v, reply=%+v", cmd, reply)
	} else if reply.Err == ErrTimeout {
		kv.SSPrintf("PushshardRPCERR from gid=%d, sid=%d:%s", args.ClientGid, args.Shard.Id, reply.Err)
	} else if reply.Err == ErrConfigStale && SDebug {
		kv.mu.Lock()
		local := kv.curConfig.Num
		kv.mu.Unlock()
		kv.SSPrintf("PushshardRPCERR from gid=%d sid=%d:%s,local=%d,remote=%d", args.ClientGid, args.Shard.Id, reply.Err, local, args.ConfigNum)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

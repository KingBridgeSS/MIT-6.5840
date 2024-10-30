package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	ClientID int64
	Seq      int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.config = ck.sm.Query(-1)
	ck.Seq = 0
	ck.ClientID = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ck.Seq++
	args := GetArgs{}
	args.Key = key
	args.ClientID = ck.ClientID
	args.Seq = ck.Seq
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		CCPrintf("getting %s", key)
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				var ok bool
				ok = srv.Call("ShardKV.Get", &args, &reply)
				// for {
				// 	ok = srv.Call("ShardKV.Get", &args, &reply)
				// 	if ok && reply.Err == ErrNotReady {
				// 		time.Sleep(100 * time.Millisecond)
				// 		// ck.Seq++
				// 		// args.Seq = ck.Seq
				// 		continue
				// 	}
				// 	break
				// }
				// CCPrintf("get reply=%+v\n", reply)
				if ok && (reply.Err == OK) || reply.Err == ErrNoKey {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.Seq++
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientID = ck.ClientID
	args.Seq = ck.Seq
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		// CCPrintf("making %s key=%s", op, key)
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				// if ok && reply.Err == ErrWrongGroup || reply.Err == ErrNotReady {

				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// if ok && reply.Err == ErrNotReady {
				// 	ck.Seq++
				// 	args.Seq = ck.Seq
				// 	break
				// }
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

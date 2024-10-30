package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const CDebug = true

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

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientID int64
	seq      int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientID = nrand()
	ck.seq = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientID = ck.clientID
	args.Seq = ck.seq
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			// CPrintf("query")
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seq++
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientID = ck.clientID
	args.Seq = ck.seq
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seq++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientID = ck.clientID
	args.Seq = ck.seq
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seq++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientID = ck.clientID
	args.Seq = ck.seq
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.seq++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

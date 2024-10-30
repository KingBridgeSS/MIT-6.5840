package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	rt       requester
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
	requester := MakeRequester(servers)
	ck.rt = *requester
	ck.clientID = nrand()
	ck.seq = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientID: ck.clientID,
		Seq:      ck.seq,
	}
	reply := GetReply{}
	context := Context{
		Rt: &ck.rt,
		Strategy: &GetRequest{
			Args:  &args,
			Reply: &reply,
			Rt:    &ck.rt,
		},
	}
	result := context.process()
	ck.seq++
	return result
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		ClientID: ck.clientID,
		Seq:      ck.seq,
	}
	reply := PutAppendReply{}
	context := Context{
		Rt: &ck.rt,
		Strategy: &PutAppendRequest{
			Rt:    &ck.rt,
			Op:    op,
			Args:  &args,
			Reply: &reply,
		},
	}
	context.process()
	ck.seq++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

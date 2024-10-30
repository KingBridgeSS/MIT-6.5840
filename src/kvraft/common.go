package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	PUT            = "Put"
	APPEND         = "Append"
)

// err
const (
	NOTALEADER = "not a leader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	ClientID int64
	Seq      int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientID int64
	Seq      int
}

type GetReply struct {
	Err   Err
	Value string
}

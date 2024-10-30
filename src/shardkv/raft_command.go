package shardkv

import "6.5840/shardctrler"

type RaftCommandType int

const (
	Operation RaftCommandType = iota
	UpdateConfig
	PushShard
	GarbageCollect
)

type RaftCommand struct {
	RaftCommandType RaftCommandType
	ConfigNum       int
	Gid             int
	Command         interface{}
}

func newRaftCommand(t RaftCommandType, configNum int, gid int, c interface{}) RaftCommand {
	return RaftCommand{
		RaftCommandType: t,
		Command:         c,
		ConfigNum:       configNum,
		Gid:             gid,
	}
}

// Operation
type OpType int

const (
	GETOP OpType = iota
	PUTOP
	APPENDOP
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   OpType
	OpKey    string
	OpValue  string
	Seq      int
	ClientID int64
}

// Update Config
// 对应的RaftCommand.ConfigNum使用的是新的Config Num
type UpdateConfigInfo struct {
	Config shardctrler.Config
}

// push shard
type PushShardInfo struct {
	Shard  Shard
	SeqMap map[int64]int
}

// gc
type GarbageInfo struct {
	ShardId int
}

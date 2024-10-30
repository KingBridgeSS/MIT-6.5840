package shardkv

import (
	"time"

	"6.5840/labrpc"
)

const LOOP_TIMEOUT = 300
const PUSH_TIMEOUT = 60000000

func (kv *ShardKV) ConfigLoop() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			loopSleep()
			// kv.SSPrintf("not a leader?")
			continue
		}
		// kv.SSPrintf("raft alive")
		kv.mu.Lock()
		// kv.SSPrintf("server alive")
		// 存在prevConfig的gid不匹配curConfig且configNum过时，说明还需要push
		redundantShardIdList := make([]int, 0)
		for shardId, gid := range kv.prevConfig.Shards {
			if gid == kv.gid && kv.curConfig.Shards[shardId] != gid && kv.shardMap[shardId].ConfigNum < kv.curConfig.Num {
				redundantShardIdList = append(redundantShardIdList, shardId)
			}
		}

		for _, shardId := range redundantShardIdList {
			pushShard := kv.shardMap[shardId].Clone(kv.curConfig.Num)
			seqMap := make(map[int64]int)
			for k, v := range kv.seqMap {
				seqMap[k] = v
			}
			args := PushShardArgs{
				Shard:     pushShard,
				ConfigNum: kv.curConfig.Num,
				SeqMap:    seqMap,
				ClientGid: kv.gid,
			}
			targetGid := kv.curConfig.Shards[shardId]
			serversGidList := kv.curConfig.Groups[targetGid]
			if SDebug {
				kv.SSPrintf("sending shardId=%d to gid=%d,Num=%d", shardId, targetGid, kv.curConfig.Num)
			}
			servers := make([]*labrpc.ClientEnd, len(serversGidList))
			for i, name := range serversGidList {
				servers[i] = kv.make_end(name)
			}
			go func(sid int) {
				i := 0 //server
				startTime := time.Now()
				l := len(servers)
				for {
					var reply PushShardReply
					ok := servers[i].Call("ShardKV.PushShard", &args, &reply)
					timeout := time.Since(startTime) >= PUSH_TIMEOUT*time.Millisecond
					// timeout = false
					if (ok && reply.Err == OK) || timeout {
						kv.mu.Lock()
						kv.SSPrintf("recv push reply from gid=%d of sid=%d: %+v", targetGid, sid, reply.Err)
						cmd := newRaftCommand(GarbageCollect, kv.curConfig.Num, kv.gid, GarbageInfo{
							ShardId: sid,
						})
						kv.mu.Unlock()
						kv.startCommand(cmd)
						if !timeout {
							kv.SSPrintf("完全删除分片shardId=%d", sid)
						} else {
							kv.SSPrintf("完全删除分片shardId=%d,timeout...", sid)
						}
						break
					} else if reply.Err == ErrConfigStale {
						break
					}
					i = (i + 1) % l
					if i == 0 {
						loopSleep()
					}
				}
			}(shardId)
		}
		if len(redundantShardIdList) > 0 {
			kv.mu.Unlock()
			loopSleep()
			// kv.SSPrintf("sleep for has sent")
			continue
		}
		// kv.SSPrintf("all sent")
		// 确保自己收到了所有需要的shard
		recv := true
		for sid, gid := range kv.prevConfig.Shards {
			if gid != kv.gid && kv.curConfig.Shards[sid] == kv.gid && kv.shardMap[sid].ConfigNum < kv.curConfig.Num {
				recv = false
			}
		}
		if !recv {
			kv.mu.Unlock()
			loopSleep()
			continue
		}
		// kv.SSPrintf("all received")
		// update config if possible
		local := kv.curConfig
		kv.mu.Unlock()
		remote := kv.mck.Query(local.Num + 1)
		if remote.Num != local.Num+1 {
			loopSleep()
			continue
		}
		kv.SSPrintf("original num=%d, 收到新config %+v", local.Num, remote)
		cmd := newRaftCommand(UpdateConfig, remote.Num, kv.gid, UpdateConfigInfo{Config: remote})
		err := kv.startCommand(cmd)
		kv.SSPrintf("updated config num=%d,err=%s", remote.Num, err)
	}
	kv.SSPrintf("config loop killed")
}
func loopSleep() {
	time.Sleep(LOOP_TIMEOUT * time.Millisecond)
}

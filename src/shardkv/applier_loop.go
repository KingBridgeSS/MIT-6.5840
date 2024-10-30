package shardkv

func (kv *ShardKV) processSeq(op Op) bool {
	seq0, ok := kv.seqMap[op.ClientID]
	if !ok {
		kv.seqMap[op.ClientID] = op.Seq
		return true
	}
	if seq0 >= op.Seq {
		kv.SSPrintf("fail processing: op=%+v,seq0=%d", op, seq0)
		return false
	}
	kv.seqMap[op.ClientID] = op.Seq
	return true
}
func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		// kv.SSPrintf("raft layer received message")
		if msg.CommandValid {
			if msg.Command == nil {
				continue
			}
			kv.mu.Lock()
			cmd := msg.Command.(RaftCommand)
			res := opResult{
				Err:       OK,
				ConfigNum: cmd.ConfigNum,
				Gid:       cmd.Gid,
			}
			switch cmd.RaftCommandType {
			case Operation:
				op := cmd.Command.(Op)
				shardId := key2shard(op.OpKey)
				if kv.curConfig.Shards[shardId] != kv.gid {
					res.Err = ErrWrongGroup
				} else if kv.shardMap[shardId].Mp == nil {
					res.Err = ErrNotReady
				} else if kv.processSeq(op) {
					switch op.OpType {
					case PUTOP:
						kv.shardMap[shardId].Mp[op.OpKey] = op.OpValue
					case APPENDOP:
						kv.shardMap[shardId].Mp[op.OpKey] += op.OpValue
					}
				}
			case UpdateConfig:
				rc := cmd.Command.(UpdateConfigInfo).Config
				if kv.curConfig.Num >= rc.Num {
					break
				}
				// 初始化从未分配变分配的shard
				for sid, gid := range rc.Shards {
					if gid == kv.gid && kv.curConfig.Shards[sid] == 0 {
						kv.shardMap[sid].Mp = make(map[string]string)
						kv.shardMap[sid].ConfigNum = rc.Num
						kv.shardMap[sid].Id = sid
					}
				}
				kv.SSPrintf("original: prev=%d,cur=%d,updated: prev=%d,cur=%d", kv.prevConfig.Num, kv.curConfig.Num, kv.curConfig.Num, rc.Num)
				kv.prevConfig = kv.curConfig
				kv.curConfig = rc
			case PushShard:
				info := cmd.Command.(PushShardInfo)
				shard := info.Shard
				if kv.curConfig.Num < cmd.ConfigNum {
					res.Err = ErrNotReady
				} else if shard.ConfigNum < kv.curConfig.Num {
					res.Err = ErrConfigStale
				} else if kv.shardMap[info.Shard.Id].Mp != nil {
					// already received, do nothing
				} else {
					kv.shardMap[shard.Id] = shard.Clone(shard.ConfigNum)
					for cid, rid := range info.SeqMap {
						if rid0, ok := kv.seqMap[cid]; !ok || rid0 < rid {
							kv.seqMap[cid] = rid
						}
					}
					// kv.SSPrintf("raft recv pushShard=%+v", info)
				}
			case GarbageCollect:
				if cmd.ConfigNum < kv.curConfig.Num {
					break
				}
				sid := cmd.Command.(GarbageInfo).ShardId
				kv.shardMap[sid].Mp = nil
				kv.shardMap[sid].ConfigNum = cmd.ConfigNum
			}
			if kv.threshold() {
				kv.takeSnapshot(msg.CommandIndex)
			}
			// ch := kv.getResultCh(msg.CommandIndex)
			ch, ok := kv.resultChMap[msg.CommandIndex]
			if ok {
				select {
				case ch <- res:
				default:
				}
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			kv.applySnapshot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

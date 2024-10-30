package shardctrler

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		msg, ok := <-sc.applyCh
		if !ok {
			return
		}
		_, isLeader := sc.rf.GetState()
		sc.mu.Lock()
		if msg.CommandValid && msg.CommandIndex > sc.lastApplied {
			op := msg.Command.(Op)
			if sc.processSeq(op) {
				switch op.OpType {
				case JOIN:
					originalConfig := sc.lastConfig()
					newShards := sc.Balance(originalConfig.Shards, originalConfig.Groups, op.Servers, nil)
					newGroup := make(map[int][]string, len(originalConfig.Groups)+len(op.Servers))
					for k, v := range originalConfig.Groups {
						newGroup[k] = make([]string, len(v))
						copy(newGroup[k], v)
					}
					for k, v := range op.Servers {
						newGroup[k] = make([]string, len(v))
						copy(newGroup[k], v)
					}
					newNum := len(sc.configs)
					newConfig := Config{
						Num:    newNum,
						Shards: newShards,
						Groups: newGroup,
					}
					sc.configs = append(sc.configs, newConfig)
				case LEAVE:
					originalConfig := sc.lastConfig()
					newShards := sc.Balance(originalConfig.Shards, originalConfig.Groups, nil, op.GIDs)
					newGroup := make(map[int][]string, len(originalConfig.Groups))
					for k, v := range originalConfig.Groups {
						newGroup[k] = make([]string, len(v))
						copy(newGroup[k], v)
					}
					for _, gid := range op.GIDs {
						delete(newGroup, gid)
					}
					newNum := len(sc.configs)
					newConfig := Config{
						Num:    newNum,
						Shards: newShards,
						Groups: newGroup,
					}
					sc.configs = append(sc.configs, newConfig)
				case MOVE:
					originalConfig := sc.lastConfig()
					newShards := originalConfig.Shards //数组是值复制，切片是引用复制
					newShards[op.Shard] = op.GID
					newGroup := make(map[int][]string, len(originalConfig.Groups))
					for k, v := range originalConfig.Groups {
						newGroup[k] = make([]string, len(v))
						copy(newGroup[k], v)
					}
					newNum := len(sc.configs)
					newConfig := Config{
						Num:    newNum,
						Shards: newShards,
						Groups: newGroup,
					}
					sc.configs = append(sc.configs, newConfig)
				}
			}
			ch, ok := sc.resultChMap[msg.CommandIndex]
			if ok {
				if isLeader {
					ch <- opResult{}
				} else {
					ch <- opResult{
						err: true,
					}
				}
			}
			sc.lastApplied = msg.CommandIndex
		}
		sc.mu.Unlock()
	}
}
func (sc *ShardCtrler) lastConfig() Config {
	return sc.configs[len(sc.configs)-1]
}
func (sc *ShardCtrler) processSeq(op Op) bool {
	seq0, ok := sc.seqMap[op.ClientID]
	if !ok {
		sc.seqMap[op.ClientID] = op.Seq
		return true
	}
	if !(seq0+1 == op.Seq) {
		return false
	}
	sc.seqMap[op.ClientID]++
	return true
}

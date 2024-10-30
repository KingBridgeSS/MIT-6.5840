package shardctrler

import (
	"fmt"

	"6.5840/utils"
)

const balanceDebug = false

// 不改变groups，在加入group前根据add或remove做出平衡预处理，返回新的shards
func (sc *ShardCtrler) Balance(shards [NShards]int, groups map[int][]string, add map[int][]string, remove []int) [NShards]int {
	// sc.SSPrintf("Num = %d", len(sc.configs))
	var shardsMapping [NShards]int
	pool := utils.Deque[int]{}
	nGroup := len(groups)
	if nGroup != 0 {
		shardsMapping = sc.preBalance(shards, groups)
	} else {
		shardsMapping = shards
	}
	shardRev := utils.NewOrderedMap[[]int]()
	for shard, gid := range shardsMapping {
		if gid == 0 {
			pool.EnqueueBack(shard)
			continue
		}
		existingShards, _ := shardRev.Get(gid)
		shardRev.Set(gid, append(existingShards, shard))
	}
	for gid := range groups {
		_, ok := shardRev.Get(gid)
		if !ok {
			shardRev.Set(gid, make([]int, 0))
		}
	}
	if len(add) != 0 {
		orderedAdd := utils.NewOrderedMap[[]string]()
		for k, v := range add {
			orderedAdd.Set(k, v)
		}
		orderedAdd.Sort()
		// sc.SSPrintf("shardRev: %s, group0: %+v, adding: %s", shardRev.String(), groups, orderedAdd.String())
		newNGroup := len(add) + nGroup
		shardsPerGroup := NShards / newNGroup
		for _, gid := range orderedAdd.Keys() {
			if _, ok := shardRev.Get(gid); !ok {
				shardRev.Set(gid, []int{})
			}
		}
		shardRev.Sort()
		// sc.SSPrintf("after sorting:%s", shardRev.String())
		keys := shardRev.Keys()
		for _, gid := range keys {
			shards, _ := shardRev.Get(gid)
			nShards := len(shards)
			for nShards > shardsPerGroup {
				lastShard := shards[nShards-1]
				shards[nShards-1] = 0
				shards = shards[:nShards-1]
				shardRev.Set(gid, shards)
				pool.EnqueueBack(lastShard)
				nShards--
			}
		}
		for _, gid := range orderedAdd.Keys() {
			var shards []int
			for i := 0; i < shardsPerGroup; i++ {
				shard, ok := pool.DequeueBack()
				if !ok {
					fmt.Println("balance warning: pool empty while adding")
					SPrintf("shardRev: %+v", shardRev)
					break
				}
				shards = append(shards, shard)
			}
			shardRev.Set(gid, shards)
		}
		pour(shardRev, &pool)
		// sc.SSPrintf("after adding: %s", shardRev.String())
	} else if len(remove) != 0 {
		// sc.SSPrintf("shardRev: %s, group0: %+v, removeGIDs: %+v", shardRev.String(), groups, remove)
		newNGroup := nGroup - len(remove)
		if newNGroup == 0 {
			return [NShards]int{}
		}
		shardRev.Sort()
		shardsPerGroup := NShards / newNGroup
		for _, removeGid := range remove {
			shards, _ := shardRev.Get(removeGid)
			for _, shard := range shards {
				pool.EnqueueBack(shard)
			}
			shardRev.Delete(removeGid)
		}
		keys := shardRev.Keys()
		for _, gid := range keys {
			shards, _ := shardRev.Get(gid)
			nShards := len(shards)
			for nShards < shardsPerGroup {
				shard, ok := pool.DequeueFront()
				if !ok {
					fmt.Println("balance warning: pool empty while removing")
				}
				shards, _ := shardRev.Get(gid)
				shardRev.Set(gid, append(shards, shard))
				nShards++
			}
		}
		pour(shardRev, &pool)
		// sc.SSPrintf("after removing: %s", shardRev.String())
	} else {
		fmt.Println("balance warning: add and remove argument is empty")
	}
	var ans [NShards]int
	keys := shardRev.Keys()
	for _, gid := range keys {
		shards, _ := shardRev.Get(gid)
		for _, shard := range shards {
			ans[shard] = gid
		}
	}
	return ans
}

func (sc *ShardCtrler) preBalance(shardsMapping [NShards]int, groups map[int][]string) [NShards]int {
	// 在增加或删除前把shards配平
	nGroup := len(groups)
	shardsPerGroup := NShards / nGroup
	pool := utils.Deque[int]{} //shards
	var lack []int             // gid
	// gid -> shard[]
	shardRev := utils.NewOrderedMap[[]int]()
	for shard, gid := range shardsMapping {
		if gid == 0 {
			pool.EnqueueBack(shard)
			continue
		}
		existingShards, _ := shardRev.Get(gid)
		shardRev.Set(gid, append(existingShards, shard))
	}
	for gid := range groups {
		if _, ok := shardRev.Get(gid); !ok {
			shardRev.Set(gid, []int{})
		}
	}
	shardRev.Sort()
	// sc.SSPrintf("before preBalance: %s", shardRev.String())
	if balanceDebug {
		fmt.Printf("shardRev: %+v\n", shardRev)
	}
	keys := shardRev.Keys()
	for _, gid := range keys {
		shards, _ := shardRev.Get(gid)
		nShards := len(shards)
		// sc.SSPrintf("gid=%d, nShards=%d, per=%d", gid, nShards, shardsPerGroup)
		for nShards > shardsPerGroup {
			lastShard := shards[nShards-1]
			shards[nShards-1] = 0
			shards = shards[:nShards-1]
			shardRev.Set(gid, shards)
			pool.EnqueueBack(lastShard)
			nShards--
		}
		for nShards < shardsPerGroup {
			shard, ok := pool.DequeueFront()
			if !ok {
				lack = append(lack, gid)
				break
			}
			shards, _ := shardRev.Get(gid)
			shardRev.Set(gid, append(shards, shard))
			nShards++
		}
	}
	for _, gid := range lack {
		shards, _ := shardRev.Get(gid)
		nShards := len(shards)
		for nShards < shardsPerGroup {
			shard, ok := pool.DequeueFront()
			if !ok {
				// sc.SSPrintf("balance warning: may not be able to do prebalancing.(lack gid: %d)", gid)
				break
			}
			shards, _ := shardRev.Get(gid)
			shardRev.Set(gid, append(shards, shard))
			nShards++
		}
	}
	if balanceDebug {
		fmt.Printf("shardRev: %+v\n", shardRev)
	}
	pour(shardRev, &pool)
	// sc.SSPrintf("after preBalance: %s", shardRev.String())
	if balanceDebug {
		fmt.Printf("shardRev: %+v\n", shardRev)
	}
	var ans [NShards]int
	keys = shardRev.Keys()
	for _, gid := range keys {
		shards, _ := shardRev.Get(gid)
		for _, shard := range shards {
			ans[shard] = gid
		}
	}
	return ans
}

// 将pool剩余的元素分配至shardRev的随机几个元素上
func pour(shardRev *utils.OrderedMap[[]int], pool *utils.Deque[int]) {
	if shardRev.Len() == 0 {
		fmt.Println("balance warning: shardRev is empty while pouring")
		return
	}
	keys := shardRev.ReversedKeys()
	for _, k := range keys {
		shard, ok := pool.DequeueFront()
		if !ok {
			break
		}
		existingShards, _ := shardRev.Get(k)
		shardRev.Set(k, append(existingShards, shard))
	}
}

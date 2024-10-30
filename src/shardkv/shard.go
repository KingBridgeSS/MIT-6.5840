package shardkv

type ShardStatus int

// const (
// 	Serving ShardStatus = iota
// 	Fetching
// 	Deleted
// )

type Shard struct {
	ConfigNum int
	Mp        map[string]string
	Id        int
}

func (sd *Shard) Clone(configNum int) Shard {
	newMp := make(map[string]string)
	for k, v := range sd.Mp {
		newMp[k] = v
	}
	return Shard{
		ConfigNum: configNum,
		Mp:        newMp,
		Id:        sd.Id,
	}
}

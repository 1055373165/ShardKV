package shardctrler

import "sort"

type CtrlerStateMachine struct {
	Configs []Config
}

func NewCtrlerStateMachine() *CtrlerStateMachine {
	cf := &CtrlerStateMachine{Configs: make([]Config, 1)}
	cf.Configs[0] = DefaultConfig()
	return cf
}

func (csm *CtrlerStateMachine) Query(num int) (Config, Err) {
	// corner check avoid arr out of range
	if num < 0 || num >= len(csm.Configs) {
		return csm.Configs[len(csm.Configs)-1], OK
	}
	return csm.Configs[num], OK
}

// adding a new group to the cluster requires dealing with load balancing afterward
func (csm *CtrlerStateMachine) Join(groups map[int][]string) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	// constructing a new configuration
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	// add the parameter groups to Groups
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	// 构造 gid -> shardid 映射关系
	// shard   gid
	// 0       1
	// 1       1
	// 2       2
	// 3       2
	// 4       1
	// transform gid -> shardid
	// gid     shardid
	// 1       0,1,4
	// 2       2,3
	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// reassign shard to a new group
	// gid    shard
	// 1      0,1,4,6
	// 2      2,3
	// 3      5,7
	// 4      []
	// move
	// first move shard 0 to group 4
	// 1      1,4,6
	// 2      2,3
	// 3      5,7
	// 4      [0]
	// second move shard 1 to group 4
	// 1      4,6
	// 2      2,3
	// 3      5,7
	// 4      [0,1]
	for {
		maxGid, minGid := gidWithMaxShards(gidToShards), gidWithMinShards(gidToShards)
		if maxGid != 0 && len(gidToShards[maxGid])-len(gidToShards[minGid]) <= 1 {
			break
		}
		// move shard from maxGid to minGid
		gidToShards[minGid] = append(gidToShards[minGid], gidToShards[maxGid][0])
		gidToShards[maxGid] = gidToShards[maxGid][1:]
	}

	// store the newly constructed configuration information in the shards array
	var newShards [NShards]int
	for gid, shards := range gidToShards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	csm.Configs = append(csm.Configs, newConfig)

	return OK
}

func (csm *CtrlerStateMachine) Leave(gids []int) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	// building new configurations
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	// gid -> shards
	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// remove the groups from the configuration
	var unassignedShards []int
	for _, gid := range gids {
		// if gid is in Group, remove it from Groups(map[gid]servers)
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		// remove the shard under the group
		if shards, ok := gidToShards[gid]; ok {
			unassignedShards = append(unassignedShards, shards...)
			delete(gidToShards, gid)
		}
	}

	var newShards [NShards]int
	// reassign the unassigned shards to the remaining groups
	if len(newConfig.Groups) != 0 {
		for _, shard := range unassignedShards {
			minGid := gidWithMinShards(gidToShards)
			gidToShards[minGid] = append(gidToShards[minGid], shard)
		}
		// resave the shards array
		for gid, shards := range gidToShards {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}

	// save the new configuration
	newConfig.Shards = newShards
	csm.Configs = append(csm.Configs, newConfig)
	return OK
}

func (csm *CtrlerStateMachine) Move(shardId, gid int) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	// building new configurations
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	// update the shard's gid
	newConfig.Shards[shardId] = gid
	// append the new configuration to the array
	csm.Configs = append(csm.Configs, newConfig)
	return OK
}

// deep copy of groups
func copyGroups(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string, len(groups))
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func gidWithMaxShards(gidToShards map[int][]int) int {
	// corner check
	if shard, ok := gidToShards[0]; ok && len(shard) > 0 {
		return 0
	}
	// In order for each node to get the same configuration when called,
	// the gid needs to be sorted to ensure that the traversal order is deterministic
	// the algorithm of sorting the map
	var gids []int
	for gid := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	maxGid, maxShards := -1, -1
	for _, gid := range gids {
		if len(gidToShards[gid]) > maxShards {
			maxGid, maxShards = gid, len(gidToShards[gid])
		}
	}
	return maxGid
}

func gidWithMinShards(gidToShards map[int][]int) int {
	var gids []int
	for gid := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	minGid, minShards := -1, NShards+1
	// filter out gid = 0
	for _, gid := range gids {
		if gid != 0 && len(gidToShards[gid]) < minShards {
			minGid, minShards = gid, len(gidToShards[gid])
		}
	}
	return minGid
}

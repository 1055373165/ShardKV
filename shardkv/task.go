package shardkv

import (
	"sync"
	"time"
)

// 处理 apply 任务
func (kv *ShardKV) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				// 如果是已经处理过的消息则直接忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var reply *OpReply
				// retrieve user action information
				raftCmd := message.Command.(RaftCommand)
				if raftCmd.CmdType == ClientOperation {
					op := raftCmd.Data.(Op)
					if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) { // Repeated requests
						reply = kv.duplicateTable[op.ClientId].Reply
					} else { // New requests are applied to the state machine
						shardId := key2shard(op.Key)
						reply = kv.applyToStateMachine(op, shardId)
						if op.OpType != OpGet {
							kv.duplicateTable[op.ClientId] = LastOperationInfo{
								SeqId: op.SeqId,
								Reply: reply,
							}
						}
					}
				} else {
					reply = kv.handleConfigChangeMessage(raftCmd)
				}

				// 将结果发送回去
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyCh := kv.getNotifyChannel(message.CommandIndex)
					notifyCh <- reply
				}

				// 判断是否需要 snapshot
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.makeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				kv.restoreFromSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// 获取当前配置
func (kv *ShardKV) fetchConfigTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {

			kv.mu.Lock()
			newConfig := kv.mck.Query(kv.currentConfig.Num + 1)
			kv.mu.Unlock()

			// pass in the raft module for cluster synchronization
			kv.ConfigCommand(RaftCommand{
				CmdType: ConfigChange,
				Data:    newConfig,
			}, &OpReply{})
		}
		time.Sleep(FetchConfigInterval)
	}
}

func (kv *ShardKV) shardMigrationTask() {

	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			// 1. 查询需要迁移的分片
			gidToShards := kv.getShardByStatus(MoveIn)
			var wg sync.WaitGroup
			for gid, shardIds := range gidToShards {
				wg.Add(1)
				// request the server cluster of the original group to obtain shard data
				// the shard number is saved in the latest configuration
				go func(servers []string, configNum int, shardIds []int) {
					defer wg.Done()
					// iterate through each node in the group and read the corresponding shard data from the leader
					getShardArgs := ShardOperationArgs{ConfigNum: configNum, ShardIds: shardIds}
					for _, server := range servers {
						var getShardReply ShardOperationReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.GetShardsData", &getShardArgs, &getShardReply)
						// the shard data is successfully obtained and data migration starts
						if ok && getShardReply.Err == OK {
							kv.ConfigCommand(RaftCommand{
								CmdType: ShardMigration,
								Data:    getShardReply,
							}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}

		time.Sleep(ShardMigrationInterval)
	}
}

// query shards based on status
func (kv *ShardKV) getShardByStatus(status ShardStatus) map[int][]int {
	gidToShards := make(map[int][]int)
	for i, shard := range kv.shards {
		if shard.Status == status {
			// original group id
			gid := kv.prevConfig.Shards[i]
			if gid != 0 {
				if _, ok := gidToShards[gid]; !ok {
					gidToShards[gid] = make([]int, 0)
				}
				gidToShards[gid] = append(gidToShards[gid], i)
			}
		}
	}
	// [1 1 1 2 2 2 3 3 3]
	// prevGroup1 [0 1 2 shard]
	return gidToShards
}

// GetShardsDate acquires shard data
func (kv *ShardKV) GetShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	// get the slice data from the leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// the current group configuration don't have the latest shard data
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	// copy shard data
	reply.ShardData = make(map[int]map[string]string)
	for _, shardId := range args.ShardIds {
		reply.ShardData[shardId] = kv.shards[shardId].copyData()
	}

	// copy duplicate table
	reply.DuplicateTable = make(map[int64]LastOperationInfo)
	for clientId, op := range kv.duplicateTable {
		reply.DuplicateTable[clientId] = op.copyData()
	}

	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

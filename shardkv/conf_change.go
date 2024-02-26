package shardkv

import (
	"kvraft2/shardctrler"
	"time"
)

func (kv *ShardKV) ConfigCommand(command RaftCommand, reply *OpReply) {
	// call raft to store the request in the raft log and synchronize the logs
	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) handleConfigChangeMessage(command RaftCommand) *OpReply {
	switch command.CmdType {
	case ConfigChange:
		newConfig := command.Data.(shardctrler.Config)
		return kv.applyNewConfig(newConfig)
	case ShardMigration:
		shardData := command.Data.(ShardOperationReply)
		return kv.applyShardMigration(&shardData)
	default:
		panic("unknown command type")
	}
}

func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	if kv.currentConfig.Num+1 == newConfig.Num {
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid { // move shard in
				gid := kv.currentConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = MoveIn
				}
			}
			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid { // move shard out
				gid := newConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = MoveOut
				}
			}
		}

		kv.prevConfig = kv.currentConfig
		kv.currentConfig = newConfig
		return &OpReply{
			Err: OK,
		}
	}

	return &OpReply{
		Err: ErrWrongConfig,
	}
}

func (kv *ShardKV) applyShardMigration(shardDataReply *ShardOperationReply) *OpReply {
	if shardDataReply.ConfigNum == kv.currentConfig.Num {
		// take out all the data that needs to be migrated
		for shardId, shardData := range shardDataReply.ShardData {
			shard := kv.shards[shardId]
			// store the data in the shard corresponding to the current Group.
			if shard.Status == MoveIn {
				for k, v := range shardData {
					shard.KV[k] = v
				}
				// the status is set to GC waiting for cleanup.
				shard.Status = GC
			} else {
				break
			}
		}

		// copy de-duplicated table data
		for clientId, dupTable := range shardDataReply.DuplicateTable {
			table, ok := kv.duplicateTable[clientId]
			// If the returned de-duplicated table data does not exist or is newer than the current de-duplicated table data,
			// then update the current de-duplicated table data
			if !ok || table.SeqId < dupTable.SeqId {
				kv.duplicateTable[clientId] = dupTable
			}
		}
	}
	return &OpReply{Err: ErrWrongConfig}
}

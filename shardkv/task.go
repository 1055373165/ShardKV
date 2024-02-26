package shardkv

import "time"

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
		kv.mu.Lock()
		newConfig := kv.mck.Query(kv.currentConfig.Num + 1)
		kv.mu.Unlock()
		// pass in the raft module for cluster synchronization
		kv.ConfigCommand(RaftCommand{
			CmdType: ConfigChange,
			Data:    newConfig,
		}, &OpReply{})
		time.Sleep(FetchConfigInterval)
	}
}

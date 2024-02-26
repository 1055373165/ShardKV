package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"kvraft2/labrpc"
	"kvraft2/shardctrler"
	"math/big"
	"time"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIds map[int]int // 记录 Leader 节点的 id，避免下一次请求的时候去轮询查找 Leader
	// clientID+seqId 确定一个唯一的命令
	clientId int64
	seqId    int64
}

// MakeClerk the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.leaderIds = make(map[int]int)
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			if _, exist := ck.leaderIds[gid]; !exist {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]

			for {
				srv := ck.make_end(servers[ck.leaderIds[gid]])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(servers)
					if ck.leaderIds[gid] == oldLeaderId {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// PutAppend shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	args.Key = key
	args.Value = value
	args.Op = op

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, exist := ck.leaderIds[gid]; !exist {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]

			for {
				srv := ck.make_end(servers[ck.leaderIds[gid]])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.seqId++
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(servers)
					if ck.leaderIds[gid] == oldLeaderId {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

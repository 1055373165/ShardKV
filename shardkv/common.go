package shardkv

import (
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

const (
	ClientRequestTimeout = 500 * time.Millisecond
	FetchConfigInterval  = 100 * time.Millisecond
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type Op struct {
	Key      string
	Value    string
	OpType   OperationType
	ClientId int64
	SeqId    int64
}

type OperationType uint8

const (
	OpGet OperationType = iota
	OpPut
	OpAppend
)

type OpReply struct {
	Value string
	Err   Err
}

func getOperationType(v string) OperationType {
	switch v {
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	default:
		panic("unknown operation type #{v}")
	}
}

type LastOperationInfo struct {
	Reply *OpReply
	SeqId int64
}

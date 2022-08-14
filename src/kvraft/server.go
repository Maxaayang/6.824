package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	CommandId int
	CommandType CommandType
	Key string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	lastApplied int
	db map[string] string
	finshSet map[int64] int
	commitCh map[int64]chan Op
	replyCh map[int]chan replyContext	// 指令处理完之后结果返回的Chanel

	clientReply map[int64] context	// 客户端指令对应的reply, 如果是读的话还要考虑重新给client回复一次，所以得进行保存

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

// 保存上次的结果
type replyContext struct {
	Value string
	Index int
	// Term int
}

// 保存上次执行的指令及其结果，用来判断是否有重复的指令
type context struct {
	commandId int
	reply replyContext
}

// type ApplyNotifyMsg struct {
// 	Value string
// }

func (kv *KVServer) ReceiveMsg() {
	for !kv.killed() {
		msg := <- kv.applyCh
		DPrintf("leader %d 接受到提交的指令 %v", kv.me, msg)
		if msg.CommandValid {
			kv.ApplyMsg(msg)
		} else if msg.SnapshotValid {

		} else {}
	}
}

// 先判断是否已经执行过了
func (kv *KVServer) ApplyMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("leader %d 已经成功接受到raft传送过来的 command %v", kv.me, msg)
	op := msg.Command.(Op)
	reply := replyContext{}
	// 如果是重复的指令的话，直接讲上次的结果返回回去
	// if command, ok := kv.clientReply[op.ClientId]; ok && command.commandId >= op.CommandId {
	// 	return
	// }

	// 如果是新的指令的话就继续执行
	DPrintf("开始更新数据库")
	if op.CommandType == PutCommand {
		kv.db[op.Key] = op.Value
		reply.Value = op.Value
	} else if op.CommandType == AppendCommand {
		if _, ok := kv.db[op.Key]; ok {
			kv.db[op.Key] += op.Value
			reply.Value = kv.db[op.Key]
		} else {
			kv.db[op.Key] = op.Value
			reply.Value = kv.db[op.Key]
		}
	} else if op.CommandType == GetCommand {
		reply.Value = kv.db[op.Key]
	}
	reply.Index = msg.CommandIndex
	DPrintf("leader %d 更新完之后的数据库为 %v", kv.me, kv.db)

	if replyCh, ok := kv.replyCh[msg.CommandIndex]; ok {
		replyCh <- reply
		DPrintf("leader %d 已经成功将操作结果 %v 返回", kv.me, reply)
	}

	// kv.clientReply[op.ClientId] = context{msg.CommandIndex, reply}
	// kv.lastApplied = msg.CommandIndex

}

func (kv *KVServer) PutAppendGet(args *PutAppendGetArgs, reply *PutAppendGetReply) {
	// Your code here.
	kv.mu.Lock()
	if args.CommandId <= kv.clientReply[args.ClientId].commandId {
		DPrintf("是重复的指令 %v, commandId %d", args, kv.clientReply[args.ClientId].commandId)
		reply.Value = kv.clientReply[args.ClientId].reply.Value
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	op := Op{args.ClientId, args.CommandId, args.Op, args.Key, args.Value}
	index, _, leader := kv.rf.Start(op)
	if !leader {
		reply.Err = ErrWrongLeader
		// DPrintf("wrong leader")
		return
	}
	DPrintf("指令 %v 已经发送给了leader %d index is %d Key %v, Value %v", op, kv.me, index, args.Key, args.Value)

	replyCh := make(chan replyContext, 1)
	kv.mu.Lock()
	kv.replyCh[index] = replyCh
	kv.mu.Unlock()
	DPrintf("replyCh[%d]已经更新", index)
	
	// 这里不能根据value来判断是否成功, 因为可能是一个空字符串
	for now := time.Now(); time.Since(now) < 500*time.Millisecond; {
		msg := <- replyCh
		if msg.Index == index {
			DPrintf("leader %d 已经成功接收到了操作返回的结果 %v", kv.me, msg)
			reply.Value = msg.Value
			reply.Err = OK
			kv.clientReply[op.ClientId] = context{args.CommandId, msg}
			kv.lastApplied = args.CommandId
			return
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string] string)
	kv.clientReply = make(map[int64] context)
	kv.replyCh = make(map[int]chan replyContext)
	kv.lastApplied = 0

	go kv.ReceiveMsg()

	// You may need initialization code here.

	return kv
}

package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"

	"6.824/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	leaderId int
	clientId int64
	commandId int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.commandId = 1
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var value string
	ck.PutAppendGet(key, &value, GetCommand)
	// log.Printf("server Get Key %v, Value %v", key, value)
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppendGet(key string, value *string, op CommandType) {
	// You will have to modify this function.
	args := PutAppendGetArgs{ck.clientId, ck.commandId, key, *value, op}
	reply := PutAppendGetReply{}
	log.Printf("server PutAppendGet send args %v, reply %v", args, reply)
	for {
		if !ck.servers[ck.leaderId].Call("KVServer.PutAppendGet", &args, &reply) || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.commandId++
		*value = reply.Value
		break
	}
	log.Printf("server PutAppendGet get args %v, reply %v", args, reply)
}

func (ck *Clerk) Put(key string, value string) {
	// log.Printf("server Put Key %v, Value %v", key, value)
	ck.PutAppendGet(key, &value, PutCommand)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppendGet(key, &value, AppendCommand)
	// log.Printf("server Append Key %v, Value %v", key, value)
}

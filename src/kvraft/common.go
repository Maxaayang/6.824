package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

)

type CommandType string

const (
	GetCommand = "Get"
	PutCommand = "Put"
	AppendCommand = "Append"
)

type Err string

// Put or Append
type PutAppendGetArgs struct {
	ClientId int64
	CommandId int
	Key   string
	Value string
	Op    CommandType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendGetReply struct {
	Err Err
	Value string
}

type GetArgs struct {
	Key string
	ClientId int64
	CommandId int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
// type JobType int
// const (
// 	MapJob = iota
// 	ReduceJob
// 	WaitingJob
// 	KillJob
// )

// type Job struct {
// 	JobType JobType
// 	InputFile []string
// 	JobId int
// 	ReducerNum int
// }

// type Condition int
// const (
// 	MapPhase = iota
// 	ReducePhase
// 	AllDone
// )

// type JobCondition int
// const (
// 	JobWorking = iota
// 	JobWaiting
// 	JobDone
// )

// type JobMetaInfo struct {
// 	condition JobCondition
// 	StartTime time.Time
// 	JobPtr *Job
// }

// type JobMetaHoder struct {
// 	MetaMap map[int]*JobMetaInfo
// }

const (
	WorkerWait = 0
	MapPhase = 1
	ReducePhase = 2
	FinshPhase = 3
)


type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Args struct {

}

type Reply struct {
	MapFile string
	ReduceFiles []string
	NReduce int
	Remain bool
	JobId int
	IntermediateFiles []string
	WorkingType int
}

// type ReqReduce struct {

// }

// type ResReduce struct {
// 	ReduceFiles []string
// 	Remain bool
// 	ReduceId int
// 	// OutFileName string
// 	WorkingType int
// }


// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}


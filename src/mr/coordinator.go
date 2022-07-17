package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	files []string
	file_num int;
	nReduce int
	filesMapDone map[string] bool
	keysReduceDone []bool
	work_id int
}



// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetUndoneMapFile(req ReqMap, res *ResMap) error {
	if (c.work_id >= c.file_num) {
		res.remain = false
		return nil
	}
	res.filename = c.files[c.work_id]
	c.work_id++
	res.nReduce = c.nReduce
	
	return nil
}

func (c *Coordinator) MapDone(req ReqMap, res *ResMap) error {
	if (res.done) {
		c.filesMapDone[res.filename] = true
	}
	return nil
}

func (c *Coordinator) GetUndoneReduceFile(req ReqReduce, res *ResReduce) error {
	
	return nil
}

func (c *Coordinator) ReduceDone(req ReqReduce, res *ResReduce) error {

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.

	return c.nReduce == len(c.keysReduceDone)

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files, len(files), nReduce, make(map[string] bool), make([]bool, nReduce), 0}

	// Your code here.


	c.server()
	return &c
}

package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"

	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu                sync.Mutex
	files             []string
	file_num          int
	nReduce           int
	mapDone           map[string]bool
	mapPhase          map[string]bool
	reduceDone        []bool
	reducePhase       []bool
	mapStartTime      []time.Time
	reduceStartTime   []time.Time
	filesIntermediate [][]string
	work_id           int
	reduceId          int
	curPhase int
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

func (c *Coordinator) GenerateJobId() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := c.work_id
	c.work_id++
	return res
}

func (c *Coordinator) GenerateReduceId() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := c.reduceId % c.nReduce
	c.reduceId++
	return res
}

// func (c *Coordinator) CrashHandler() {}ƒ

// TODO 这里还得优化，没有检查中间的没有完成的任务
func (c *Coordinator) GetUndoneMapFile(req *Args, res *Reply) error {
	if c.curPhase != MapPhase {
		return nil
	}
	c.mu.Lock()
	if c.work_id < c.file_num {
		c.mu.Unlock()
		res.JobId = c.GenerateJobId()
		res.MapFile = c.files[res.JobId]
		// c.mu.Unlock()
		fmt.Println("Find an Undo map1: ", res.JobId, res.MapFile)
		res.NReduce = c.nReduce
		c.mapStartTime[res.JobId] = time.Now()
		res.Remain = true
		res.WorkingType = MapPhase

		// go c.CheckMapDone(res.MapFile)
		// c.mu.Unlock()

		return nil
	}
	c.mu.Unlock()

	// TODO 这一部分需要重新设计一下
	fmt.Println(time.Now())
	time.Sleep(20 * time.Second)
	fmt.Println(time.Now())
	for _, file := range c.files {
		c.mu.Lock()
		if !c.mapDone[file] {
			c.mu.Unlock()
			res.MapFile = file
			fmt.Println("Find an Undo map2: ", res.JobId, res.MapFile)
			res.JobId = c.GenerateJobId()
			res.NReduce = c.nReduce
			// c.mapStartTime[res.JobId] = time.Now()
			c.mapStartTime = append(c.mapStartTime, time.Now())
			res.Remain = true
			res.WorkingType = MapPhase

			// go c.CheckMapDone(res.MapFile)
			// c.mu.Unlock()

			return nil
		}
		c.mu.Unlock()
	}
	c.curPhase = ReducePhase
	res.Remain = false
	return nil

}

// Map任务完成，保存中间文件
func (c *Coordinator) MapDone(res *Reply, req *Args) error {
	fmt.Println("Set Map Done: ", res.JobId, res.MapFile)
	// fmt.Println(time.Now())
	// fmt.Println(time.Now().Sub(res.StartTime))
	// fmt.Println(res.StartTime)
	if time.Since(c.mapStartTime[res.JobId]) <= 10 * time.Second {
		fmt.Println("Map Done: ", res.JobId, res.MapFile)
		// c.mu.Lock()
		// defer c.mu.Unlock()
		c.mapDone[res.MapFile] = true
		// fmt.Println("IntermediateFiles.size: ", len(res.IntermediateFiles))
		for i := 0; i < c.nReduce; i++ {
			// fmt.Println(i)
			c.filesIntermediate[i] = append(c.filesIntermediate[i], res.IntermediateFiles[i])
		}
	}

	return nil
}

func (c *Coordinator) GetUndoneReduceFile(req *Args, res *Reply) error {
	if c.curPhase != ReducePhase {
		return nil
	}
	if c.reduceId < c.nReduce {
		res.Remain = true
		res.JobId = c.GenerateReduceId()
		// for i := 0; i < len(c.filesIntermediate[res.ReduceId]); i++ {
		// 	res.Filenames = append(res.Filenames, c.filesIntermediate[res.ReduceId][i])
		// }
		res.ReduceFiles = c.filesIntermediate[res.JobId]
		c.reduceStartTime[res.JobId] = time.Now()
		res.WorkingType = ReducePhase
		// go c.CheckReduceDone(res.JobId)
		fmt.Println("Find an Undo reduce1: ", res.JobId)
		return nil
	}

	time.Sleep(10 * time.Second)
	for i := 0; i < c.nReduce; i++ {
		c.mu.Lock()
		if !c.reduceDone[i] {
			c.mu.Unlock()
			res.Remain = true
			res.JobId = i
			res.ReduceFiles = c.filesIntermediate[i]
			c.reduceStartTime[i] = time.Now()
			res.WorkingType = ReducePhase
			// go c.CheckReduceDone(res.JobId)
			fmt.Println("Find an Undo reduce2: ", res.JobId)
			return nil
		}
		c.mu.Unlock()
	}

	c.curPhase = FinshPhase
	res.WorkingType = FinshPhase
	res.Remain = false
	return nil
}

func (c *Coordinator) GetUndoFile(req *Args, res *Reply) error {
	if c.curPhase == MapPhase {
		c.GetUndoneMapFile(req, res)
	} else if c.curPhase == ReducePhase {
		c.GetUndoneReduceFile(req, res)
	} else {
		res.WorkingType = FinshPhase
	}
	return nil
}

func (c *Coordinator) ReduceDone(res *Reply, req *Args) error {
	if time.Since(c.reduceStartTime[res.JobId]) <= 10 * time.Second {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.reduceDone[res.JobId] = true
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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

func (c *Coordinator) CheckMapDone(mapFile string) {
	fmt.Println("Start Check Map Done: ", mapFile)
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapDone[mapFile] {
		c.mapPhase[mapFile] = true
		fmt.Println("Map Done: ", mapFile)
	}
}

func (c *Coordinator) CheckReduceDone(reduceId int) {
	fmt.Println("Start Check Reduce Done: ", reduceId)
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceDone[reduceId] {
		c.reducePhase[reduceId] = true
		fmt.Println("Reduce Done")
	}
}

func (c *Coordinator) CheckAllDone() bool {
	// Your code here.
	// fmt.Println("Start Check All Done")
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, file := range c.files {
		if !c.mapDone[file] {
			return false
		}
	}

	for i := 0; i < c.nReduce; i++ {
		if !c.reduceDone[i] {
			return false
		}
	}
	fmt.Println("All Finsh")
	return true

}

func (c *Coordinator) Done() bool {
	return c.CheckAllDone()
}

func (c *Coordinator) ss() {
	fmt.Println("work_id", c.work_id)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// fmt.Println("files: ", files)
	c.files = files
	c.file_num = len(files)
	c.nReduce = nReduce
	c.mapDone = make(map[string]bool, len(files))
	c.mapPhase = make(map[string]bool, len(files))
	c.reduceDone = make([]bool, nReduce)
	c.reducePhase = make([]bool, nReduce)
	c.mapStartTime = make([]time.Time, len(files))
	c.reduceStartTime = make([]time.Time, nReduce)
	c.work_id = 0
	c.reduceId = 0
	c.curPhase = MapPhase
	// const i int = 8
	c.filesIntermediate = make([][]string, nReduce)
	for i := 0; i < nReduce; i++ {
		c.filesIntermediate[i] = make([]string, c.file_num)
	}
	// fmt.Println("filesIntermediate: ", len(c.filesIntermediate), len(c.filesIntermediate[0]))
	// Your code here.

	c.server()
	go c.ss()
	return &c
}

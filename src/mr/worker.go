package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"time"

	// "plugin"
	"os"
	// "time"
	"encoding/json"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	
	// i := true
	Loop:
	for {
		reply := findUndoFile()

		switch reply.WorkingType {
		case MapPhase:
			runMap(mapf, reply)
		case ReducePhase:
			runReduce(reducef, reply)
		case WorkerWait:
			fmt.Println("wait 1 s")
			time.Sleep(time.Second)
		case FinshPhase:
			fmt.Println("Finsh!!!")
			// i = false
			break Loop
		default:
			fmt.Println("This should not happened!")
			// i = false
			break Loop
		}
		
	}

}

func saveMap(kva *[]KeyValue, resmap *Reply) {
	HashedKV := make([][]KeyValue, resmap.NReduce)

	for _, kv := range *kva {
		HashedKV[ihash(kv.Key)%resmap.NReduce] = append(HashedKV[ihash(kv.Key)%resmap.NReduce], kv)
	}

	for i := 0; i < resmap.NReduce; i++ {
		oname := "mr-tmp-" + strconv.Itoa(resmap.JobId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
		resmap.IntermediateFiles = append(resmap.IntermediateFiles, string(oname))
		// fmt.Println("IntermediateFiles: ", resmap.IntermediateFiles)
		// fmt.Println(oname)
	}
	// fmt.Println("IntermediateFiles.size: ", len(resmap.IntermediateFiles))
	// fmt.Println("IntermediateFiles: ", resmap.IntermediateFiles)
}

// func saveReduce() {}

func findUndoFile() Reply {
	args := Args{}
	reply := Reply{}
	// fmt.Println("find undo map file")
	call("Coordinator.GetUndoFile", &args, &reply)
	return reply
}

func runMap(mapf func(string, string) []KeyValue, resmap Reply) {
	// fmt.Println("filename: ", resmap.Filename)
	file, err := os.Open(resmap.MapFile)
	if err != nil {
		log.Fatalf("cannot open %v", resmap.MapFile)
	}
	// fmt.Println("open file: ", resmap.Filename)
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", resmap.MapFile)
	}
	// fmt.Println("read file: ", resmap.Filename)
	file.Close()
	// fmt.Println("close file", resmap.JobId)
	kva := mapf(resmap.MapFile, string(content))
	// fmt.Println("kva", resmap.JobId)
	sort.Sort(ByKey(kva))
	// fmt.Println("saveMap", resmap.JobId)
	saveMap(&kva, &resmap)
	fmt.Println("call map done", resmap.JobId)
	// fmt.Println("StartTime: ", resmap.StartTime)
	reqmap := Args{}

	call("Coordinator.MapDone", &resmap, &reqmap)
	// time.Sleep(time.Second)
}

// func findUndoReduceFile() Reply {
// 	reqreduce := Args{}
// 	resreduce := Reply{}
// 	call("Coordinator.GetUndoneReduceFile", &reqreduce, &resreduce)
// 	return resreduce
// }

func runReduce(reducef func(string, []string) string, resreduce Reply) {
	var kva []KeyValue
	// fmt.Println(resreduce.ReduceFiles)
	for _, fileName := range resreduce.ReduceFiles {
		// fmt.Println("i: ", i)
		file, _ := os.Open(fileName)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	// fmt.Println("kva: ", kva)

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", resreduce.JobId)
	os.Rename(tempFile.Name(), oname)
	// resreduce.OutFileName = oname
	reqreduce := Args{}
	call("Coordinator.ReduceDone", &resreduce, &reqreduce)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close() // 延迟语句的执行顺序

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

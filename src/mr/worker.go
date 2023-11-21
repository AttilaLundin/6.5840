package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

type KeyValueSlice []KeyValue

// for sorting by key.
func (a KeyValueSlice) Len() int           { return len(a) }
func (a KeyValueSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyValueSlice) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	printIfError(err)
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		reply := RequestTask()
		if reply.Filename == "nil" && reply.Status != DONE {
			fmt.Println("Filename is empty")
			// todo: kill
		}
		switch reply.Status {
		case MAP_PHASE:
			MapTask(reply, mapf)
		case REDUCE_PHASE:
			ReduceTask(reply, reducef)
		case DONE:
			return
		}
		time.Sleep(time.Second)
	}

}

func MapTask(replyMap *TaskReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(replyMap.Filename)
	printIfError(err)

	content, err := io.ReadAll(file)
	printIfError(err)

	err = file.Close()
	printIfError(err)

	var mapResult KeyValueSlice = mapf(replyMap.Filename, string(content))
	sort.Sort(mapResult)

	intermediateFiles := make([]*os.File, replyMap.NReduce)
	intermediateFilePaths := make([]string, replyMap.NReduce)
	encs := make([]*json.Encoder, replyMap.NReduce)

	// create intermediate files and json encoders so that we can encode them into json docs
	for i := 0; i < replyMap.NReduce; i++ {

		intermediateFileName := "mr-" + strconv.Itoa(replyMap.TaskNumber) + "-" + strconv.Itoa(i)
		tmpFile, err := os.Create(intermediateFileName)
		printIfError(err)

		//store the pointer to the intermediate file
		intermediateFiles[i] = tmpFile
		//store the file directory
		intermediateFilePaths[i] = "../main" + intermediateFileName
		encs[i] = json.NewEncoder(tmpFile)
	}

	//partition the output from map (using ihash) into intermediate files with json for further work
	for _, kv := range mapResult {
		err = encs[ihash(kv.Key)%replyMap.NReduce].Encode(kv)
		printIfError(err)
	}

	args := SignalPhaseDoneArgs{IntermediateFiles: make([]IntermediateFile, len(intermediateFilePaths)), Status: DONE}
	for i, path := range intermediateFilePaths {
		args.IntermediateFiles[i].Path = path
		args.IntermediateFiles[i].ReduceTaskNumber = i
		args.IntermediateFiles[i].filename = replyMap.Filename
	}

	reply := TaskReply{}

	mapSuccess := SignalPhaseDone(args, reply)
	if !mapSuccess {
		println("Signalling map done failed")
	}
}

func ReduceTask(replyReduce *TaskReply, reducef func(string, []string) string) {

	// implement reduce
	// json decode from replyReduce.filename
	rFile, err := os.Open(replyReduce.Filename)
	printIfError(err)

	var reduceKV []KeyValue
	dec := json.NewDecoder(rFile)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}

		reduceKV = append(reduceKV, kv)
	}

	group := make(map[string][]string)

	for i := 0; i < len(reduceKV); i++ {
		group[reduceKV[i].Key] = append(group[reduceKV[i].Key], reduceKV[i].Value)
	}

	oname := "mr-out-" + strconv.Itoa(replyReduce.TaskNumber)
	ofile, _ := os.Create(oname)

	for key, values := range group {
		output := reducef(key, values)

		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	ofile.Close()

	args := SignalPhaseDoneArgs{Status: DONE}
	reply := TaskReply{}

	reduceSuccess := SignalPhaseDone(args, reply)
	if !reduceSuccess {
		println("Signalling reduce done failed")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func RequestTask() *TaskReply {
	args := GetTaskArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.GrantTask", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.Filename)
		return &reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return &reply
}

func MapSignalPhaseDone(args interface{}, reply TaskReply) bool {

	ok := call("Coordinator.MapPhaseDoneSignalled", &reply, &args)
	if ok {
		fmt.Println("Coordinator has been signalled")
		return true
	} else {
		fmt.Printf("call failed!\n")
		return false
	}

}
func ReduceSignalPhaseDone(args interface{}, reply TaskReply) bool {

	ok := call("Coordinator.ReducePhaseDoneSignalled", &reply, &args)
	if ok {
		fmt.Println("Coordinator has been signalled")
		return true
	} else {
		fmt.Printf("call failed!\n")
		return false
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func logIfFatalError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func printIfError(err error) {
	if err != nil {
		fmt.Println(err)
		return
	}
}

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
	"sync"
	"time"
)

var lock = sync.Mutex{}

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
		time.Sleep(time.Second)
		reply := RequestTask()
		if reply.FailedTask == true {
			println("This is a failed task!")
		}

		switch reply.Status {
		case MAP_PHASE:
			if reply.Filename == "" {
				println("empty filename... continue,", reply.Filename, reply.TaskNumber, reply.Status, reply.FailedTask)
				continue
			}
			MapTask(reply, mapf)
		case REDUCE_PHASE:
			if len(reply.IntermediateFiles) == 0 {
				println("empty reduce task... continue")
				continue
			}
			ReduceTask(reply, reducef)
		case DONE:
			os.Exit(0)
		}
		time.Sleep(time.Second)
	}

}

func RequestTask() *Task {
	args := GetTaskArgs{}
	reply := Task{}

	ok := call("Coordinator.GrantTask", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.Filename)
		return &reply
	} else {
		fmt.Printf("call failed! Trying again\n")
	}
	return &reply
}

func MapTask(replyMap *Task, mapf func(string, string) []KeyValue) {

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

	// create intermediate Files and json encoders so that we can encode them into json docs
	for i := 0; i < replyMap.NReduce; i++ {

		intermediateFileName := "mr-" + strconv.Itoa(replyMap.TaskNumber) + "-" + strconv.Itoa(i)
		tmpFile, err := os.Create(intermediateFileName)
		printIfError(err)

		//store the pointer to the intermediate file
		intermediateFiles[i] = tmpFile
		//store the file directory
		intermediateFilePaths[i] = intermediateFileName
		encs[i] = json.NewEncoder(tmpFile)
	}

	//partition the output from map (using ihash) into intermediate Files with json for further work
	for _, kv := range mapResult {
		err = encs[ihash(kv.Key)%replyMap.NReduce].Encode(kv)
		printIfError(err)
	}

	args := SignalPhaseDoneArgs{FileName: replyMap.Filename, IntermediateFiles: make([]IntermediateFile, len(intermediateFilePaths))}
	for i, path := range intermediateFilePaths {
		args.Task = replyMap
		args.IntermediateFiles[i].Path = path
		args.IntermediateFiles[i].ReduceTaskNumber = i
		args.IntermediateFiles[i].Filename = replyMap.Filename
		args.Status = REDUCE_PHASE
	}

	reply := Task{}

	mapSuccess := MapSignalPhaseDone(args, reply)
	if !mapSuccess {
		println("Signalling map done failed")
	}
}

func ReduceTask(replyReduce *Task, reducef func(string, []string) string) {

	var reduceKV []KeyValue

	filesToReduce := make([]*os.File, len(replyReduce.IntermediateFiles))
	decs := make([]*json.Decoder, len(replyReduce.IntermediateFiles))

	for i, intermediateFile := range replyReduce.IntermediateFiles {
		file, err := os.Open(intermediateFile.Path)
		printIfError(err)
		filesToReduce[i] = file
		decs[i] = json.NewDecoder(file)
		defer file.Close()
	}

	for _, decoder := range decs {
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			reduceKV = append(reduceKV, kv)
		}
	}

	group := make(map[string][]string)

	for i := 0; i < len(reduceKV); i++ {
		group[reduceKV[i].Key] = append(group[reduceKV[i].Key], reduceKV[i].Value)
	}

	oname := "mr-out-" + strconv.Itoa(replyReduce.TaskNumber)
	ofile, _ := os.Create(oname)

	for key, values := range group {
		output := reducef(key, values)
		_, err := fmt.Fprintf(ofile, "%v %v\n", key, output)
		if err != nil {
			log.Fatal("ERROR IN WORKER, could not write key:", key, " output: ", output)
		}
	}
	err := ofile.Close()
	printIfError(err)

	args := SignalPhaseDoneArgs{Task: replyReduce, ReduceTaskNumber: replyReduce.TaskNumber, Status: DONE}
	reply := Task{}

	reduceSuccess := ReduceSignalPhaseDone(args, reply)
	if !reduceSuccess {
		fmt.Println("Signalling reduce done failed")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func MapSignalPhaseDone(args SignalPhaseDoneArgs, reply Task) bool {

	ok := call("Coordinator.MapPhaseDoneSignalled", &args, &reply)
	if ok {
		fmt.Println("Coordinator has been signalled From Map")
		return true
	} else {
		fmt.Printf("call failed!\n")
		return false
	}

}
func ReduceSignalPhaseDone(args SignalPhaseDoneArgs, reply Task) bool {

	ok := call("Coordinator.ReducePhaseDoneSignalled", &args, &reply)
	if ok {
		fmt.Println("Coordinator has been signalled from Reduce")
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

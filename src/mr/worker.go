package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"hash/fnv"
	"io"
	"io/ioutil"
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
		time.Sleep(time.Second)
		reply := RequestTask()
		if reply.FailedTask == true {
			println("worker received previously failed task")
		}

		switch reply.Status {
		case MAP_PHASE:
			if reply.Filename == "" {
				println("empty filename - request task again", reply.Filename, reply.TaskNumber, reply.Status, reply.FailedTask)
				continue
			}
			getFileFromS3(reply.Filename)
			MapTask(reply, mapf)
		case REDUCE_PHASE:
			if len(reply.IntermediateFiles) == 0 {
				println("empty reduce task - request task again")
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

func getFileFromS3(filename string) {
	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)

	// Create the GetObject input parameters
	input := &s3.GetObjectInput{
		Bucket: aws.String("tda596-group10"), // Replace with your bucket name
		Key:    aws.String(filename),         // Replace with the object key
	}

	// Call S3 to retrieve the object
	result, err := client.GetObject(context.TODO(), input)
	if err != nil {
		log.Fatalf("unable to retrieve object, %v", err)
	}

	// Create a file to write the downloaded contents to
	outFile, err := os.Create(filename)
	if err != nil {
		log.Fatalf("unable to create file, %v", err)
	}
	defer outFile.Close()

	// Write the contents of S3 Object to the file
	_, err = io.Copy(outFile, result.Body)
	if err != nil {
		log.Fatalf("unable to write file, %v", err)
	}
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
		tmpFile, err := ioutil.TempFile("", intermediateFileName)
		printIfError(err)

		//store the pointer to the intermediate file
		intermediateFiles[i] = tmpFile
		//store the file directory
		intermediateFilePaths[i] = tmpFile.Name()
		encs[i] = json.NewEncoder(tmpFile)
	}

	//partition the output from map (using ihash) into intermediate Files with json for further work
	for _, kv := range mapResult {
		err = encs[ihash(kv.Key)%replyMap.NReduce].Encode(kv)
		printIfError(err)
	}

	for i, tmpFile := range intermediateFiles {
		err = tmpFile.Close()
		printIfError(err)

		// Use os.Rename to atomically rename the temporary file to the final destination
		err = os.Rename(tmpFile.Name(), intermediateFilePaths[i])
		printIfError(err)
	}

	args := SignalPhaseDoneArgs{FileName: replyMap.Filename, IntermediateFiles: make([]IntermediateFile, len(intermediateFilePaths))}
	for i, path := range intermediateFilePaths {
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

	tmpFile, err := ioutil.TempFile("", "mr-out-")
	printIfError(err)

	defer tmpFile.Close()

	for key, values := range group {
		output := reducef(key, values)
		_, err := fmt.Fprintf(tmpFile, "%v %v\n", key, output)
		if err != nil {
			log.Fatal("ERROR IN WORKER, could not write key:", key, " output: ", output)
		}
	}

	err = os.Rename(tmpFile.Name(), oname)
	printIfError(err)

	args := SignalPhaseDoneArgs{ReduceTaskNumber: replyReduce.TaskNumber, Status: DONE}
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
	coordinatorIP := os.Getenv("CIP")
	c, err := rpc.DialHTTP("tcp", coordinatorIP+":1234")
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

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
	// main loop of worker
	for {
		reply := RequestTask()
		// a task that has been restarted by a new worker due to a crash gets marked with FailedTask
		if reply.FailedTask == true {
			println("worker received previously failed task")
		}

		switch reply.Status {
		case MAP_PHASE:
			if reply.Filename == "" {
				println("empty filename - request task again")
				// restart for loop to ask for a new task
				continue
			}
			// AWS method to retrieve a file from our AWS S3 bucket
			getFileFromS3(reply.Filename)
			MapTask(reply, mapf)
		case REDUCE_PHASE:
			// if the length of intermediateFiles is 0 from the reply, it means it is an invalid reduce task
			if len(reply.IntermediateFiles) == 0 {
				println("empty reduce task - request task again")
				// restart for loop to ask for a new task
				continue
			}
			ReduceTask(reply, reducef)
		case DONE:
			// a reply with DONE from coordinator
			println("worker received DONE - exiting")
			os.Exit(0)
		}
		// sleep so next task request gets time to ready up
		time.Sleep(time.Second)
	}

}

func RequestTask() *Task {
	args := GetTaskArgs{}
	reply := Task{}

	ok := call("Coordinator.GrantTask", &args, &reply)
	if ok {
		fmt.Printf("%v\n", reply.Filename)
		return &reply
	} else {
		fmt.Printf("call failed! Trying again\n")
	}
	return &reply
}

func getFileFromS3(filename string) {
	// load the shared AWS Configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// create a service client for the S3 bucket
	client := s3.NewFromConfig(cfg)

	// create the GetObject input parameters
	input := &s3.GetObjectInput{
		Bucket: aws.String("group10-tda596"), // the S3 bucket where files are shared
		Key:    aws.String(filename),         // file we want to retrieve task for
	}

	// retrieve object from S3 using input with bucket name and filename as key
	result, err := client.GetObject(context.TODO(), input)
	if err != nil {
		log.Fatalf("unable to retrieve object, %v", err)
	}

	// create a file to write the downloaded contents to
	outFile, err := os.Create(filename)
	if err != nil {
		log.Fatalf("unable to create file, %v", err)
	}
	defer outFile.Close()

	// write the contents of S3 object to the file
	_, err = io.Copy(outFile, result.Body)
	if err != nil {
		log.Fatalf("unable to write file, %v", err)
	}
}

// the function responsible for mapping files and creates intermediate files for further computations
func MapTask(replyMap *Task, mapf func(string, string) []KeyValue) {

	file, err := os.Open(replyMap.Filename)
	printIfError(err)

	content, err := io.ReadAll(file)
	printIfError(err)

	err = file.Close()
	printIfError(err)

	// the key value pairs stored in a slice
	var mapResult KeyValueSlice = mapf(replyMap.Filename, string(content))
	sort.Sort(mapResult)

	// create nReduce nr of (intermediate) files
	intermediateFiles := make([]*os.File, replyMap.NReduce)
	// create nReduce nr of paths for each intermediate file
	intermediateFilePaths := make([]string, replyMap.NReduce)
	// create intermediate Files and json encoders (one for each file) so that we can encode them into json docs
	encs := make([]*json.Encoder, replyMap.NReduce)

	for i := 0; i < replyMap.NReduce; i++ {
		// create file name according to convention mr-X-Y where X = map task nr and Y == reduce task nr
		intermediateFileName := "mr-" + strconv.Itoa(replyMap.TaskNumber) + "-" + strconv.Itoa(i)
		// tmp file "trick" to avoid observing incomplete files
		tmpFile, err := ioutil.TempFile("", intermediateFileName)
		printIfError(err)

		// store the pointer to the intermediate file
		intermediateFiles[i] = tmpFile
		// store the file directory
		intermediateFilePaths[i] = tmpFile.Name()
		encs[i] = json.NewEncoder(tmpFile)
	}

	//partition the output from map (using ihash) into intermediate Files with json for further work
	for _, kv := range mapResult {
		err = encs[ihash(kv.Key)%replyMap.NReduce].Encode(kv)
		printIfError(err)
	}

	// close each tmpFile
	for i, tmpFile := range intermediateFiles {
		err = tmpFile.Close()
		printIfError(err)

		// atomically rename the temporary file to the final destination
		// part of the "trick" to avoid observing incomplete files
		err = os.Rename(tmpFile.Name(), intermediateFilePaths[i])
		printIfError(err)
	}

	// for each path, initialize each intermediate files in each slot of the slice
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

// the function responsible for reducing the mapped files to an output file
func ReduceTask(replyReduce *Task, reducef func(string, []string) string) {

	//data structure to store the key-value pairs read from the intermediate file
	var reduceKV []KeyValue

	//list of opened intermediate files coordinator has allocated to the worker
	filesToReduce := make([]*os.File, len(replyReduce.IntermediateFiles))

	//list of json decoders so that we can read the intermediate files
	decs := make([]*json.Decoder, len(replyReduce.IntermediateFiles))

	//opens the allocated files and created json decoders associated with them, then stores them in the datastructures above
	for i, intermediateFile := range replyReduce.IntermediateFiles {
		file, err := os.Open(intermediateFile.Path)
		printIfError(err)
		filesToReduce[i] = file
		decs[i] = json.NewDecoder(file)
		defer file.Close()
	}

	//decodes i.e. retrieves each key-value pair from the files related to this task and stores them for further computations
	for _, decoder := range decs {
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			reduceKV = append(reduceKV, kv)
		}
	}

	//data structure that will store each key, where the value will be the value associated with it and the length the sum of occurrences of said key-value pair
	group := make(map[string][]string)

	//does the grouping explained above.
	for i := 0; i < len(reduceKV); i++ {
		group[reduceKV[i].Key] = append(group[reduceKV[i].Key], reduceKV[i].Value)
	}

	//output file name
	oname := "mr-out-" + strconv.Itoa(replyReduce.TaskNumber)

	// same "trick" as in map task to prevent incomplete files
	tmpFile, err := ioutil.TempFile("", "mr-out-")
	printIfError(err)

	defer tmpFile.Close()

	//writes the result from reducf() to the temporary file. this is done for each key in group
	for key, values := range group {
		output := reducef(key, values)
		_, err := fmt.Fprintf(tmpFile, "%v %v\n", key, output)
		if err != nil {
			log.Fatal("ERROR IN WORKER, could not write key:", key, " output: ", output)
		}
	}

	//atomically creates a normal .txt file from the temporary file as in map task
	err = os.Rename(tmpFile.Name(), oname)
	printIfError(err)

	//initiate the structs containing relevant information that the coordinator needs
	args := SignalPhaseDoneArgs{ReduceTaskNumber: replyReduce.TaskNumber, Status: DONE}
	reply := Task{}

	//signals the coordinator that the task is completed
	reduceSuccess := ReduceSignalPhaseDone(args, reply)
	if !reduceSuccess {
		fmt.Println("Signalling reduce done failed")
	}
}

// will signal the coordinator that the mapping is done for the allocated task through an rpc call
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

// will signal the coordinator that the reduce is done for the allocated task through an rpc call
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

// helper function that moves error checking elsewhere.
// used to reduce clutter
func logFatalIfError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// helper function that moves error checking elsewhere.
// used to reduce clutter
func printIfError(err error) {
	if err != nil {
		fmt.Println(err)
		return
	}
}

// here lies "failiour" rip <3 🪦🌷💀

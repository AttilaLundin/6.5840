package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Status int64

type Coordinator struct {
	nReduce           int
	files             []string
	mapTasks          map[string]Task
	reduceTasks       map[int]Task
	intermediateFiles map[int][]IntermediateFile
	status            Status
}

const (
	MAP_PHASE    Status = 0
	REDUCE_PHASE        = 1
	DONE                = 2
)

type Task struct {
	ReduceBucket int
	filename     string
	status       Status
}

var taskNr = 0

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GrantTask(args *GetTaskArgs, reply *TaskReply) error {

	fmt.Println("Status in GrantTask:", c.status)

	switch c.status {

	case MAP_PHASE:
		if taskNr < len(c.files) {
			reply.Status = MAP_PHASE
			reply.Filename = c.files[taskNr]
			reply.NReduce = c.nReduce
			reply.TaskNumber = taskNr
			taskNr += 1
		} else {
			return errors.New("Map task not available")
		}

	case REDUCE_PHASE:
		if taskNr < c.nReduce {
			reply.Status = REDUCE_PHASE
			reply.IntermediateFiles = c.intermediateFiles[taskNr]
			reply.TaskNumber = taskNr
			taskNr += 1
		} else {
			return errors.New("Reduce task not available")
		}
	case DONE:
		fmt.Println("MapReduce done !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
		// TODO
	}
	return nil
}

func (c *Coordinator) MapPhaseDoneSignalled(args *SignalPhaseDoneArgs, reply *TaskReply) error {

	for _, intermediateFile := range args.IntermediateFiles {
		c.intermediateFiles[intermediateFile.ReduceTaskNumber] = append(c.intermediateFiles[intermediateFile.ReduceTaskNumber], intermediateFile)
	}

	c.mapTasks[args.FileName] = Task{filename: args.FileName, status: REDUCE_PHASE}
	c.checkMapPhaseDone()

	return nil
}

func (c *Coordinator) ReducePhaseDoneSignalled(args *SignalPhaseDoneArgs, reply *TaskReply) error {

	c.reduceTasks[args.ReduceTaskNumber] = Task{ReduceBucket: args.ReduceTaskNumber, status: DONE}
	c.checkReducePhaseDone()
	return nil
}

func (c *Coordinator) checkMapPhaseDone() {

	for _, task := range c.mapTasks {
		if task.status != REDUCE_PHASE {
			println("mapping not done yet")
			return
		}
	}

	c.status = REDUCE_PHASE
	taskNr = 0
}

func (c *Coordinator) checkReducePhaseDone() {

	for _, task := range c.reduceTasks {
		println("Task id: ", task.ReduceBucket)
		println("Task status: ", task.status)

		if task.status != DONE {
			println("reduce not done yet")
			return
		}
	}
	c.status = DONE
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	if c.status == DONE {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		nReduce:           nReduce,
		mapTasks:          make(map[string]Task),
		reduceTasks:       make(map[int]Task),
		status:            MAP_PHASE,
		intermediateFiles: make(map[int][]IntermediateFile),
	}

	for _, file := range files {
		c.mapTasks[file] = Task{status: MAP_PHASE, filename: file}
	}

	for i := 0; i < nReduce; i++ {
		c.intermediateFiles[i] = make([]IntermediateFile, len(files))
		c.reduceTasks[i] = Task{ReduceBucket: i, status: REDUCE_PHASE}
	}

	c.server()
	return &c
}

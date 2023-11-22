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
	done              bool
	tasks             map[string]Task
	intermediateFiles map[int][]IntermediateFile
	status            Status
}

const (
	MAP_PHASE    Status = 0
	REDUCE_PHASE        = 1
	DONE                = 2
)

type Task struct {
	filename string
	status   Status
}

var taskNr = 0

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GrantTask(args *GetTaskArgs, reply *TaskReply) error {

	fmt.Println("Status in GranTask:", c.status)

	switch c.status {

	case MAP_PHASE:
		if taskNr < len(c.files) {
			reply.Filename = c.files[taskNr]
			reply.TaskNumber = taskNr
			reply.NReduce = c.nReduce
			reply.Status = MAP_PHASE
			taskNr += 1
		} else {
			return errors.New("Map task not available")
		}

	case REDUCE_PHASE:
		if taskNr < c.nReduce {
			reply.IntermediateFiles = c.intermediateFiles[taskNr]
			reply.NReduce = c.nReduce
			reply.Status = REDUCE_PHASE
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

	ok := c.updateTaskStatus(args.FileName, REDUCE_PHASE)
	if !ok {
		fmt.Println("update task failed in MAP signal")
	}

	c.checkPhase(REDUCE_PHASE)

	return nil
}

func (c *Coordinator) ReducePhaseDoneSignalled(args *SignalPhaseDoneArgs, reply *TaskReply) error {
	ok := c.updateTaskStatus(args.FileName, DONE)
	if !ok {
		fmt.Println("update task failed in REDUCE signal")
	}
	c.checkPhase(DONE)
	return nil
}

func (c *Coordinator) updateTaskStatus(filename string, newStatus Status) bool {

	c.tasks[filename] = Task{filename: filename, status: newStatus}

	return true
}

func (c *Coordinator) checkPhase(nextStatus Status) {

	for _, task := range c.tasks {
		println("task.filename", task.filename)
		println("nextStatus", nextStatus)
		println("task.status", task.status)
		if task.status != nextStatus {
			return
		}
	}

	switch nextStatus {
	case REDUCE_PHASE:
		c.updateTask()
		c.status = REDUCE_PHASE
		taskNr = 0
		return
	case DONE:
		c.status = DONE
		c.done = true
	default:
		fmt.Println("i have a confeshan to make")
	}
}

func (c *Coordinator) updateTask() {
	c.tasks = make(map[string]Task)

	for _, ifiles := range c.intermediateFiles {
		for _, ifile := range ifiles {
			c.tasks[ifile.Filename] = Task{filename: ifile.Filename, status: REDUCE_PHASE}
		}
	}

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

	if c.done == true {
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
		done:              false,
		tasks:             make(map[string]Task),
		status:            MAP_PHASE,
		intermediateFiles: make(map[int][]IntermediateFile),
	}

	for _, file := range files {
		c.tasks[file] = Task{status: MAP_PHASE, filename: file}
	}

	for i := 0; i < nReduce; i++ {
		c.intermediateFiles[i] = make([]IntermediateFile, len(files))
	}

	c.server()
	return &c
}

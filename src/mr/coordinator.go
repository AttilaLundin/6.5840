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
			reply.intermediateFile = c.intermediateFiles[taskNr]
			reply.NReduce = c.nReduce
			reply.Status = REDUCE_PHASE
			taskNr += 1
		} else {
			return errors.New("Reduce task not available")
		}

	case DONE:
		fmt.Println("MapReduce done")
		// TODO

	}

	return nil
}

func (c *Coordinator) updateTaskStatus(filename string, newStatus Status) bool {
	task, ok := c.tasks[filename]
	if !ok {
		return false
	}
	task.status = newStatus
	return true
}

func (c *Coordinator) MapPhaseDoneSignalled(args *SignalPhaseDoneArgs, reply *TaskReply) error {
	for _, intermediateFile := range args.IntermediateFiles {
		c.intermediateFiles[intermediateFile.ReduceTaskNumber] = append(c.intermediateFiles[intermediateFile.ReduceTaskNumber], intermediateFile)
		ok := c.updateTaskStatus(intermediateFile.filename, args.Status)
		if !ok {
			//	TODO: handle error
		}
	}
	c.checkPhase(args.Status)
	return nil
}

func (c *Coordinator) ReducePhaseDoneSignalled(args *SignalPhaseDoneArgs, reply *TaskReply) error {
	for _, intermediateFile := range args.IntermediateFiles {
		ok := c.updateTaskStatus(intermediateFile.filename, args.Status)
		if !ok {
			//	TODO: handle error
		}
	}
	c.checkPhase(args.Status)
	return nil
}

func (c *Coordinator) checkPhase(status Status) {
	for _, task := range c.tasks {
		if task.status != status {
			return
		}
	}

	switch status {
	case REDUCE_PHASE:
		c.status = REDUCE_PHASE
		taskNr = 0
		return
	case DONE:
		c.Done()
	default:
		fmt.Println("you should not get here")
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//

func (c *Coordinator) Done() bool {
	ret := false

	if c.done == true {
		ret = true
	}
	// question: what is the entire job?

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nReduce: nReduce,
		done:    false,
		tasks:   make(map[string]Task, len(files)),
		status:  MAP_PHASE,
	}

	for _, file := range files {
		c.tasks[file] = Task{status: MAP_PHASE, filename: file}
	}

	c.server()
	return &c
}

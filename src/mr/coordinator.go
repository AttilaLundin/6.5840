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

	fmt.Println("in GranTask:", c.status)

	switch c.status {

	case MAP_PHASE:
		if taskNr < len(c.files) {
			reply.Filename = c.files[taskNr]
			reply.TaskNumber = taskNr
			reply.NReduce = c.nReduce
			//reply.Status = MAP_PHASE
			taskNr += 1
		} else {
			return errors.New("Map task not available")
		}

	case REDUCE_PHASE:
		if taskNr < c.nReduce {
			reply.intermediateFiles = c.intermediateFiles[taskNr]
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

func (c *Coordinator) MapPhaseDoneSignalled(args *SignalPhaseDoneArgs, reply *TaskReply) error {

	for i, intermediateFile := range args.IntermediateFiles {
		c.intermediateFiles[intermediateFile.ReduceTaskNumber] = append(c.intermediateFiles[intermediateFile.ReduceTaskNumber], intermediateFile)
		fmt.Println(args.IntermediateFiles[i].filename)
		fmt.Println(i)
		ok := c.updateTaskStatus(intermediateFile.filename, REDUCE_PHASE)
		if !ok {
			//	TODO: handle error
		}
	}
	fmt.Println("before checkPhase")
	fmt.Println(c.tasks["pg-being_ernest.txt"].status)
	c.checkPhase(REDUCE_PHASE)
	fmt.Println("after checkPhase")
	return nil
}

func (c *Coordinator) ReducePhaseDoneSignalled(args *SignalPhaseDoneArgs, reply *TaskReply) error {
	for _, intermediateFile := range args.IntermediateFiles {
		ok := c.updateTaskStatus(intermediateFile.filename, DONE)
		if !ok {
			//	TODO: handle error
		}
	}
	c.checkPhase(DONE)
	return nil
}

func (c *Coordinator) updateTaskStatus(filename string, newStatus Status) bool {
	fmt.Println("this is updateStatus")
	fmt.Println(c.tasks[filename])

	c.tasks[filename] = Task{filename: filename, status: newStatus}
	fmt.Println("this is updateStatus part 2")
	fmt.Println(c.tasks[filename])
	return true
}

func (c *Coordinator) checkPhase(nextStatus Status) {
	fmt.Println("this is checkphase")
	fmt.Println("nextStatus:", nextStatus)
	for i := 0; i < len(c.tasks); i++ {
		fmt.Println("c.tasks[\"pg-being_ernest.txt\"].status", c.tasks["pg-being_ernest.txt"].status)
	}

	for i, task := range c.tasks {
		fmt.Println("-----------", i)
		fmt.Println("status for the task=", task.status)
		if task.status != nextStatus {
			fmt.Println("here")
			return
		}
	}
	fmt.Println("nextStatus:::", nextStatus)
	switch nextStatus {
	case REDUCE_PHASE:
		fmt.Println("in checkPhase -> REDUCEPHASE")
		c.status = REDUCE_PHASE
		taskNr = 0
		return
	case DONE:
		c.Done()
	default:
		fmt.Println("you are ge")
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
		tasks:             make(map[string]Task, len(files)),
		status:            MAP_PHASE,
		intermediateFiles: make(map[int][]IntermediateFile),
	}

	for i, file := range files {
		fmt.Println(i)
		c.tasks[file] = Task{status: MAP_PHASE, filename: file}
		c.intermediateFiles[i] = make([]IntermediateFile, nReduce)
	}

	fmt.Println(len(c.intermediateFiles))
	c.server()
	return &c
}

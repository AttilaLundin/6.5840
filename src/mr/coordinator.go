package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type Status int64

// definitions for the coordinator and workers to use - primary purpose is for human readability and minimize error proneness
const (
	MAP_PHASE    Status = 0
	REDUCE_PHASE        = 1
	DONE                = 2
)

/*
containerization of all the relevant data the coordinator needs to have in order
to do the coordinating tasks to the workers and in order to assign work to the worker
and all the tasks that the coordinator stores and then assigns to the worker which the worker will
then use to coordinate their work assigned by the coordinator to coordinate properly and efficiently
while the workers are working together to coordinate due to concurrency requirements so that the coordinator can then
assign the correct work to the correct worker so that the workers do not interefere with each others' work while working
in parallel in this distributed system of workers and coordinators that need to work and coordinate together.
*/
type Coordinator struct {
	NrReduce          int
	Files             []string
	MapTasks          map[string]Task
	ReduceTasks       map[int]Task
	IntermediateFiles map[int][]IntermediateFile
	FailedTasks       chan *Task
	Status            Status
	lock              sync.Mutex
}

var taskNr = 0

// Your code here -- RPC handlers for the worker to call.

// Grants tasks to workers upon requests through RPC calls
func (c *Coordinator) GrantTask(args *GetTaskArgs, reply *Task) error {

	// lock shared data
	c.lock.Lock()
	defer c.lock.Unlock()

	//in case of crash, the crashed task is once again assigned to the worker
	//otherwise a new task is assigned
	select {
	case crashedTask := <-c.FailedTasks:
		*reply = *crashedTask
		reply.FailedTask = true
		go c.checkCrash(reply)
		println("in case crashedTask: ", reply.Filename, reply.TaskNumber, reply.Status, reply.Success, reply.FailedTask)
		return nil
	default:
		// switch based on the status of the coordinator
		switch c.Status {

		case MAP_PHASE:
			// check if taskNr is in range and then construct the reply
			if taskNr < len(c.Files) {
				reply.Status = MAP_PHASE
				reply.Filename = c.Files[taskNr]
				reply.NReduce = c.NrReduce
				reply.TaskNumber = taskNr
				// start a go rutin to check if the worker crashed
				go c.checkCrash(reply)
				taskNr += 1
			} else {
				//if all the map tasks are assigned but not yet completed we will dismiss the worker until we have
				//tasks to assign
				return errors.New("map task not available")
			}

		case REDUCE_PHASE:
			if taskNr < c.NrReduce {
				reply.Status = REDUCE_PHASE
				reply.IntermediateFiles = c.IntermediateFiles[taskNr]
				reply.TaskNumber = taskNr
				go c.checkCrash(reply)
				taskNr += 1
			} else {
				//if all the map tasks are assigned but not yet completed we will dismiss the worker until we have
				//tasks to assign
				return errors.New("reduce task not available")
			}
		case DONE:
			println("Work is completely finished - exiting worker")
			reply.Status = DONE
		}
	}
	return nil
}

func (c *Coordinator) MapPhaseDoneSignalled(args *SignalPhaseDoneArgs, reply *Task) error {

	c.lock.Lock()
	for _, intermediateFile := range args.IntermediateFiles {
		existingFiles := c.IntermediateFiles[intermediateFile.ReduceTaskNumber]
		fileExists := false
		for _, existingFile := range existingFiles {
			if existingFile == intermediateFile {
				fileExists = true
				break
			}
		}
		if !fileExists {
			c.IntermediateFiles[intermediateFile.ReduceTaskNumber] = append(existingFiles, intermediateFile)
		}
	}
	c.MapTasks[args.FileName] = Task{Filename: args.FileName, Status: REDUCE_PHASE, Success: true}
	c.checkMapPhaseDone()
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) ReducePhaseDoneSignalled(args *SignalPhaseDoneArgs, reply *Task) error {

	c.lock.Lock()
	c.ReduceTasks[args.ReduceTaskNumber] = Task{TaskNumber: args.ReduceTaskNumber, Status: DONE, Success: true}
	c.checkReducePhaseDone()
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) checkMapPhaseDone() {

	for _, task := range c.MapTasks {
		if task.Status != REDUCE_PHASE {
			return
		}
	}

	c.Status = REDUCE_PHASE
	taskNr = 0
}

func (c *Coordinator) checkReducePhaseDone() {

	for _, task := range c.ReduceTasks {
		// if all tasks are not DONE, simply return
		if task.Status != DONE {
			return
		}
	}
	c.Status = DONE
}

func (c *Coordinator) checkCrash(taskInfo *Task) {
	time.Sleep(time.Second * 10)

	var eqvTask Task
	var printMsg string
	switch taskInfo.Status {
	case MAP_PHASE:
		c.lock.Lock()
		eqvTask = c.MapTasks[taskInfo.Filename]
		c.lock.Unlock()
		printMsg = "worker crashed in map"
	case REDUCE_PHASE:
		c.lock.Lock()
		eqvTask = c.ReduceTasks[taskInfo.TaskNumber]
		c.lock.Unlock()
		printMsg = "worker crashed in reduce"
	}

	if !eqvTask.Success {
		c.FailedTasks <- taskInfo
		println(printMsg)
	}

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	c.lock.Lock()
	if c.Status == DONE {
		ret = true
	}
	c.lock.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NrReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:             files,
		NrReduce:          nReduce,
		MapTasks:          make(map[string]Task),
		ReduceTasks:       make(map[int]Task),
		Status:            MAP_PHASE,
		IntermediateFiles: make(map[int][]IntermediateFile),
		FailedTasks:       make(chan *Task),
		lock:              sync.Mutex{},
	}

	for _, file := range files {
		c.MapTasks[file] = Task{Status: MAP_PHASE, Filename: file}
	}

	for i := 0; i < len(c.Files); i++ {
		c.IntermediateFiles[i] = make([]IntermediateFile, 0)
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{TaskNumber: i, Status: REDUCE_PHASE}
	}

	c.server()
	return &c
}

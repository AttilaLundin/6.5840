package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Status int64

type Coordinator struct {
	NrReduce          int
	Files             []string
	MapTasks          map[string]Task
	ReduceTasks       map[int]Task
	IntermediateFiles map[int][]IntermediateFile
	FailedTasks       chan Task
	Status            Status
	lock              sync.Mutex
}

const (
	MAP_PHASE    Status = 0
	REDUCE_PHASE        = 1
	DONE                = 2
)

//type Task struct {
//	TaskNumber  int
//	filename    string
//	status      Status
//	timeOfStart time.Time
//}

var taskNr = 0

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GrantTask(args *GetTaskArgs, reply *Task) error {

	c.lock.Lock()
	defer c.lock.Unlock()

	select {
	case crashedTask := <-c.FailedTasks:
		reply = &crashedTask
		go c.checkCrash(&crashedTask)
		return nil
	default:
	}

	switch c.Status {

	case MAP_PHASE:
		if taskNr < len(c.Files) {
			reply.Status = MAP_PHASE
			reply.Filename = c.Files[taskNr]
			reply.NReduce = c.NrReduce
			reply.TaskNumber = taskNr
			taskNr += 1
		} else {
			return errors.New("Map task not available")
		}

	case REDUCE_PHASE:
		if taskNr < c.NrReduce {
			reply.Status = REDUCE_PHASE
			reply.IntermediateFiles = c.IntermediateFiles[taskNr]
			reply.TaskNumber = taskNr
			taskNr += 1
		} else {
			return errors.New("Reduce task not available")
		}
	}
	return nil
}

func (c *Coordinator) MapPhaseDoneSignalled(args *SignalPhaseDoneArgs, reply *Task) error {

	c.lock.Lock()
	for _, intermediateFile := range args.IntermediateFiles {
		c.IntermediateFiles[intermediateFile.ReduceTaskNumber] = append(c.IntermediateFiles[intermediateFile.ReduceTaskNumber], intermediateFile)
	}
	c.MapTasks[args.FileName] = Task{Filename: args.FileName, Status: REDUCE_PHASE}
	c.checkMapPhaseDone()
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) ReducePhaseDoneSignalled(args *SignalPhaseDoneArgs, reply *Task) error {

	c.lock.Lock()
	c.ReduceTasks[args.ReduceTaskNumber] = Task{TaskNumber: args.ReduceTaskNumber, Status: DONE}
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

		if task.Status != DONE {
			return
		}
	}
	c.Status = DONE
}

func (c *Coordinator) checkCrash(taskInfo *Task) {
	var initialStatus Status = taskInfo.Status
	time.Sleep(time.Second * 10)
	var newStatus Status = taskInfo.Status

	if initialStatus == newStatus {
		c.FailedTasks <- *taskInfo
		println("worker crashed in map OR reduce")
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
		FailedTasks:       make(chan Task),
		lock:              sync.Mutex{},
	}

	for _, file := range files {
		c.MapTasks[file] = Task{Status: MAP_PHASE, Filename: file}
	}

	for i := 0; i < len(files); i++ {
		c.IntermediateFiles[i] = make([]IntermediateFile, 0)
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{TaskNumber: i, Status: REDUCE_PHASE}
	}

	c.server()
	return &c
}

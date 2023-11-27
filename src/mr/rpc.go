package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

// in our implementation this struct is not neede
type GetTaskArgs struct {
}

// represents all the information related to an intermediate file
type IntermediateFile struct {
	Filename         string
	ReduceTaskNumber int
	Path             string
}

// contains all the relevant information that the coordinator will use after a task is completed
type SignalPhaseDoneArgs struct {
	FileName          string
	ReduceTaskNumber  int
	IntermediateFiles []IntermediateFile
	Status            Status
	TaskNumber        int
}

// this is the task, which can be either a map- or a reduce task
type Task struct {
	Filename          string
	TaskNumber        int
	NReduce           int
	Status            Status
	Success           bool
	FailedTask        bool
	IntermediateFiles []IntermediateFile
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

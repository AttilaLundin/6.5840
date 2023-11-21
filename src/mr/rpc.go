package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// todo: justera eventuellt
type GetTaskArgs struct {
}

type IntermediateFile struct {
	filename         string
	ReduceTaskNumber int
	Path             string
}

type SignalPhaseDoneArgs struct {
	IntermediateFiles []IntermediateFile
	Status            Status
}

type TaskReply struct {
	Filename          string
	TaskNumber        int
	NReduce           int
	Status            Status
	intermediateFiles []IntermediateFile
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

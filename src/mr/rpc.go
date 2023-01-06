package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MessageCode int

const SUCCESS MessageCode = 400
const FAIL MessageCode = 500

type WorkerType int
const NOTYPE WorkerType = 0
const MAPPER WorkerType = 1
const REDUCER WorkerType = 2

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkerArgs struct {
}

type WorkerReply struct {
	FileName    []string
	WorkerIndex int
	// 1: mapper, 2: reducer
	WorkerType WorkerType
	NReduce int
}

type CompletionArgs struct {
	WorkerIndex int
	WorkerType WorkerType
}

type CompletionRely struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

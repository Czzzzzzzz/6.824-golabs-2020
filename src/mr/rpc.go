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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type WorkerType int

const MapperType WorkerType = 1
const ReducerType WorkerType = 2

// Add your RPC definitions here.
type WorkerArgs struct {
	WorkerName string
}

type WorkerReply struct {
	FileName    string
	WorkerType  WorkerType
	WorkerIndex int
}

// proto is used when mapper completes the job.
type MapperJobArgs struct {
	WorkerType  WorkerType
	WorkerIndex int
}

type MapperJobReply struct {
	Status MessageCode
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

package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type WorkerStatus int

const Idle WorkerStatus = 1
const InProgress WorkerStatus = 2
const Completed WorkerStatus = 3

type MapperInfo struct {
	status      WorkerStatus
	workerName  string
	workerIndex int
	fileName    string
}

type ReducerInfo struct {
	status      WorkerStatus
	workerName  string
	workerIndex int
	fileName    string
}

type Master struct {
	// Your definitions here.
	files    []File
	mappers  []MapperInfo
	reducers []ReducerInfo
}

type File struct {
	fileName    string
	workerIndex int
	status      WorkerStatus
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AskTask(args *WorkerArgs, reply *WorkerReply) error {

	// if args.WorkerType == MapperType {
	// 	mapperInfo := MapperInfo{}
	// 	mapperInfo.status = InProgress
	// 	mapperInfo.workerIndex = m.AssinIndex()
	// 	mapperInfo.fileName = m.files[mapperInfo.workerIndex].fileName

	// 	m.mappers = append(m.mappers, mapperInfo)

	// 	reply.FileName = mapperInfo.fileName
	// 	reply.WorkerIndex = mapperInfo.workerIndex

	// 	log.Printf("Assign task to mapper.worker index: %d,file name: %s", reply.WorkerIndex, reply.FileName)

	// } else if args.WorkerType == ReducerType {
	// 	reducerInfo := ReducerInfo{}
	// 	reducerInfo.status = InProgress
	// 	reducerInfo.workerIndex = 1
	// }

	// check for mappers
	if m.IdleMapperExisted() {
		reply.WorkerType = MapperType
		reply.WorkerIndex = m.AssignIndex()
	}

	// check for reducers
	if m.mapperStageEnds() && m.IdleReducerExisted() {
		reply.WorkerType = ReducerType
		reply.WorkerIndex = m.AssignIndex()
	}

	return nil
}

func (m *Master) CompleteTask(args *MapperJobArgs, reply *MapperJobReply) error {
	if args.WorkerType == MapperType {
		m.files[args.WorkerIndex].status = Completed
		m.mappers[args.WorkerIndex].status = Completed
		reply.Status = SUCCESS
	}

	return nil
}

func (m *Master) AssignIndex() int {
	if m.IdleMapperExisted() {
		return len(m.mappers)
	} else {
		return -1
	}
}

func (m *Master) IdleMapperExisted() bool {
	return len(m.files) != len(m.mappers)
}

func (m *Master) IdleReducerExisted() bool {
	return false
}

func (m *Master) mapperStageEnds() bool {
	return false
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.
	for i := 0; i < len(m.files); i = i + 1 {
		file := m.files[i]
		if !m.isFileCompleted(&file) {
			return false
		}
	}

	return true
}

func (m *Master) isFileCompleted(file *File) bool {
	if file.status == Completed {
		return true
	} else {
		return false
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	// initialize files
	for i := 0; i < len(files); i = i + 1 {
		file := File{}
		file.fileName = files[i]
		file.workerIndex = -1
		m.files = append(m.files, file)
	}

	// initialize mapper
	for i := 0; i < len(files); i = i + 1 {
		mapper := MapperInfo{}
		mapper.status = Idle
		mapper.workerName = "mapper"
		mapper.fileName = ""
		mapper.workerIndex = -1
	}

	// initialize reducer
	for i := 0; i < nReduce; i = i + 1 {
		reducer := ReducerInfo{}
		reducer.status = Idle
		reducer.workerName = "reducer"
		reducer.fileName = ""
		reducer.workerIndex = -1
	}

	m.server()
	return &m
}

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

	// check for mappers
	if m.IdleMapperExisted() {
		reply.WorkerType = MapperType
		reply.WorkerIndex = m.AssignIndex()
		reply.FileName = m.AssignFile(reply.WorkerIndex)
		// reply.nReducerNum = len(m.reducers)
		reply.nReducerNum = len(m.reducers)
		log.Printf("Assign mapper. WorkerType: %d, WorkerIndex: %d, FileName: %s, nReducerNum: %d", reply.WorkerType, reply.WorkerIndex, reply.FileName, reply.nReducerNum)
	} else if m.mapperStageEnds() && m.IdleReducerExisted() { // check for reducers
		reply.WorkerType = ReducerType
		reply.WorkerIndex = m.AssignIndex()
		log.Printf("Assign reducer. WorkerType: %d, WorkerIndex: %d", reply.WorkerType, reply.WorkerIndex)
	}

	return nil
}

func (m *Master) CompleteTask(args *MapperJobArgs, reply *MapperJobReply) error {
	if args.WorkerType == MapperType {
		m.files[args.WorkerIndex].status = InProgress
		m.mappers[args.WorkerIndex].status = Completed
		reply.Status = SUCCESS
	}

	return nil
}

func (m *Master) AssignIndex() int {
	if m.IdleMapperExisted() {
		return m.getIdelMapperWorkerIndex()
	} else if m.IdleReducerExisted() {
		return m.getIdelReduerWorkerIndex()
	} else {
		return -1
	}
}

func (m *Master) AssignFile(workerIndex int) string {
	return m.files[workerIndex].fileName
}

func (m *Master) IdleMapperExisted() bool {
	if m.getIdelMapperWorkerIndex() == -1 {
		return false
	} else {
		return true
	}
}

func (m *Master) getIdelMapperWorkerIndex() int {
	for i := 0; i < len(m.mappers); i = i + 1 {
		if m.mappers[i].status == Idle {
			return i
		}
	}
	return -1
}

func (m *Master) getIdelReduerWorkerIndex() int {
	for i := 0; i < len(m.reducers); i = i + 1 {
		if m.reducers[i].status == Idle {
			return i
		}
	}
	return -1
}

func (m *Master) IdleReducerExisted() bool {
	for i := 0; i < len(m.reducers); i = i + 1 {
		if m.reducers[i].status == Idle {
			return true
		}
	}
	return false
}

func (m *Master) mapperStageEnds() bool {
	for i := 0; i < len(m.mappers); i = i + 1 {
		if m.mappers[i].status != Completed {
			return false
		}
	}
	return true
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
		m.mappers = append(m.mappers, mapper)
	}

	// initialize reducer
	for i := 0; i < nReduce; i = i + 1 {
		reducer := ReducerInfo{}
		reducer.status = Idle
		reducer.workerName = "reducer"
		reducer.fileName = ""
		reducer.workerIndex = -1
		m.reducers = append(m.reducers, reducer)
	}

	m.server()
	return &m
}

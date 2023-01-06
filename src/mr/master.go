package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type WorkerStatus int
const UNSTARTED WorkerStatus = 0
const WIP WorkerStatus = 1
const DONE WorkerStatus = 2


type WorkerInfo struct {
	// 0: unstarted, 1: wip, 2: done
	status WorkerStatus
	files []string

	// timestamp for starting point	
	ts int
}

type Master struct {
	// Your definitions here.
	inputFiles    []string
	nReduce int

	mappers  []WorkerInfo
	reducers []WorkerInfo

	mapDone int
	reduceDone int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AskTask(args *WorkerArgs, reply *WorkerReply) error {

	// reply.FileName = m.inputFiles[m.mapDone]	
	workerType := m.assignTaskType()

	reply.WorkerType = workerType

	log.Printf("assign worker type: %d", workerType)

	if workerType == MAPPER {
		m.assignMapTask(reply)
	} else if workerType == REDUCER {
	} else {
	}

	return nil
}

func (m *Master) CompleteTask(args *CompletionArgs, reply *CompletionRely) error {

	workerType := args.WorkerType
	workerIndex := args.WorkerIndex
	if workerType == MAPPER {
		log.Printf("worker %d has done.", workerIndex)
		m.mappers[workerIndex].status = DONE
		m.mapDone += 1
	} else if workerType == REDUCER {
		
	} else {
	}

	
	return nil
}

func (m *Master) assignTaskType() WorkerType {

	if !m.isMapStageEnds() && m.idleMapTaskExists() {
		return MAPPER
	} else if !m.isReduceStageEnds() && m.idleReduceTaskExists() {
		return REDUCER
	} else {
		return NOTYPE
	}
}

func (m *Master) idleMapTaskExists() bool {
	idx := m.getFirstIdleMapTask()
	if idx == -1 {
		return false
	} else {
		return true
	}
}

func (m *Master) getFirstIdleMapTask() int {
	for i := 0; i < len(m.mappers); i = i + 1 {
		if m.mappers[i].status == UNSTARTED {
			return i
		}
	}

	return -1
}

func (m *Master) isMapStageEnds() bool {

	if m.mapDone < len(m.inputFiles) {
		return false
	} else {
		return true
	}
}

func (m *Master) assignMapTask(reply *WorkerReply) error {

	idleTaskIdx := m.getFirstIdleMapTask()
	if idleTaskIdx != -1 {
		reply.WorkerIndex = idleTaskIdx
		reply.FileName = append(reply.FileName, m.inputFiles[idleTaskIdx])
		reply.NReduce = m.nReduce
		
		log.Printf("file name: %s", reply.FileName)

		m.mappers[idleTaskIdx].status = WIP
		m.mappers[idleTaskIdx].ts = 1
	}

	return nil
}

func (m *Master) idleReduceTaskExists() bool {
	for i := 0; i < len(m.reducers); i = i + 1 {
		if m.reducers[i].status == UNSTARTED {
			return true
		}
	}

	return false
}

func (m *Master) isReduceStageEnds() bool {

	if m.reduceDone < m.nReduce {
		return false
	} else {
		return true
	}
}

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

	// // Your code here.
	// for i := 0; i < len(m.files); i = i + 1 {
	// 	file := m.files[i]
	// 	if !m.isFileCompleted(&file) {
	// 		return false
	// 	}
	// }

	// return true
	return false
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
		file := files[i]
		m.inputFiles = append(m.inputFiles, file)
	}

	m.nReduce = nReduce
	m.mapDone = 0
	m.reduceDone = 0

	// initialize mapper
	for i := 0; i < len(files); i = i + 1 {
		mapper := WorkerInfo{}
		mapper.status = UNSTARTED
		mapper.files = append(mapper.files, m.inputFiles[i])
		mapper.ts = -1
		m.mappers = append(m.mappers, mapper)
	}

	m.server()
	return &m
}

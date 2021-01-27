package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import "os"
import "io/ioutil"

import "encoding/json"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// args := WorkerArgs{}
	// args.WorkerName = "my_mapper"
	// args.WorkerType = MapperType
	reply := requestMaster()

	// uncomment to send the Example RPC to the master.
	// CallExample()

	if reply.WorkerType == MapperType {
		fileName := reply.FileName
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		content, err := ioutil.ReadAll(file)
		file.Close()
		// kva := mapf(fileName, string(content))
		// fmt.Print(kva)
		kva := mapf(fileName, string(content))
		// saveIntermediaResutls(kva, reply.WorkerIndex, reply.nReducerNum)
		saveIntermediaResutls(kva, reply.WorkerIndex, 10)
		log.Printf("Worker %d completed the task.", reply.WorkerIndex)

		mapperJobArgs := MapperJobArgs{MapperType, reply.WorkerIndex}
		mapperJobReply := completeTask(&mapperJobArgs)

		if mapperJobReply.Status == SUCCESS {
			log.Printf("Master successfuly recieved message from work %d", reply.WorkerIndex)
		}
	} else if reply.WorkerType == ReducerType {
		log.Print("reducer")
	} else {
		log.Print("unassigned")
	}
}

func completeTask(args *MapperJobArgs) MapperJobReply {
	reply := MapperJobReply{}
	call("Master.CompleteTask", &args, &reply)
	return reply
}

func requestMaster() WorkerReply {
	args := WorkerArgs{}
	reply := WorkerReply{}
	call("Master.AskTask", &args, &reply)
	return reply
}

func saveIntermediaResutls(kva []KeyValue, workerIndex int, nReduecerNum int) {
	log.Printf("nReducerNum: %d, workerIndex: %d", nReduecerNum, workerIndex)
	fileName2Encoder := make(map[string](*json.Encoder))
	// var files []File
	log.Printf("after defining initialize encoder")
	for i := 1; i <= workerIndex; i = i + 1 {
		for reducerIndex := 1; reducerIndex <= nReduecerNum; reducerIndex = reducerIndex + 1 {
			fileName := fmt.Sprintf("mr-%d-%d", i, reducerIndex)
			log.Printf("fileName: %s", fileName)
			file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				log.Fatalf("cannot open %v", fileName)
			}
			defer file.Close()
			// defer file.Close()
			enc := json.NewEncoder(file)
			fileName2Encoder[fileName] = enc
		}
	}

	log.Printf("Initialize encoder")

	for i := 0; i < len(kva); i = i + 1 {
		reducerTaskNum := ihash(kva[i].Key) % nReduecerNum
		fileName := fmt.Sprintf("mr-%d-%d", workerIndex, reducerTaskNum)
		fileName2Encoder[fileName].Encode(kva[i])
		// log.Print(fileName)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

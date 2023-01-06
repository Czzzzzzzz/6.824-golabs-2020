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
	reply := requestMaster()
	
	log.Printf("fileName: %s", reply.FileName)
	log.Printf("worker type: %d", reply.WorkerType)
	
	fileName := reply.FileName
	wt := reply.WorkerType 
	nReduce := reply.NReduce
	workerIndex := reply.WorkerIndex
	// uncomment to send the Example RPC to the master.
	// CallExample()
	if reply.WorkerType == MAPPER {
		intermediate := []KeyValue{}
		for _, filename := range fileName {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)
		}
		log.Printf("ret: %v", intermediate)

		saveIntermediaResutls(intermediate, workerIndex, nReduce)
		
		args := CompletionArgs{}
		args.WorkerIndex = workerIndex
		args.WorkerType = wt
		completeMapTask(args)

	} else if reply.WorkerType == REDUCER {

	} else {

	}
}

func requestMaster() WorkerReply {
	args := WorkerArgs{}
	reply := WorkerReply{}
	call("Master.AskTask", &args, &reply)
	return reply
}

func completeMapTask(args CompletionArgs) error {
	reply := CompletionRely{}
	call("Master.CompleteTask", &args, &reply)
	return nil
}

func saveIntermediaResutls(kva []KeyValue, workerIndex int, nReduecerNum int) {
	log.Printf("nReducerNum: %d, workerIndex: %d", nReduecerNum, workerIndex)
	fileName2Encoder := make(map[string](*json.Encoder))
	// var files []File
	log.Printf("after defining initialize encoder")
	for reducerIndex := 0; reducerIndex < nReduecerNum; reducerIndex = reducerIndex + 1 {
		fileName := fmt.Sprintf("mr-%d-%d", workerIndex, reducerIndex)
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

	log.Printf("Initialize encoder")
 
	for i := 0; i < len(kva); i = i + 1 {
		reducerTaskNum := ihash(kva[i].Key) % nReduecerNum
		fileName := fmt.Sprintf("mr-%d-%d", workerIndex, reducerTaskNum)
		// log.Printf("kva: %s", kva[i])
		fileName2Encoder[fileName].Encode(kva[i])
	}

	log.Printf("Written.")
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

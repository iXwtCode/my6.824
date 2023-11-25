package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		taskReply := TaskInfo{}
		finishReply := FinishReply{}
		ok := call("Coordinator.Task", GetTaskArgs{}, &taskReply)
		if !ok {
			log.Fatal("call return false!")
		}
		taskInfo := taskReply
		switch taskInfo.TaskType {
		case Map:
			{
				//fmt.Printf("file name: %v, taskNO: %v\n", taskInfo.FileNames, taskInfo.TaskNo)
				doMap(mapf, &taskInfo)
				call("Coordinator.Finish", &taskInfo, &finishReply)
			}
		case Reduce:
			{
				doReduce(reducef, &taskInfo)
				call("Coordinator.Finish", &taskInfo, &finishReply)
			}
		case PleaseExit:
			{
				return
			}
		case Wait:
			time.Sleep(time.Second)
		default:
			return
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, info *TaskInfo) {
	filename := info.FileNames[0]
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content))
	//log.Printf("kva length: %v", len(kva))
	sort.Sort(ByKey(kva))

	reducerNum := info.ReducerNum
	hashKV := make([][]KeyValue, reducerNum)
	for _, kv := range kva {
		nthReducer := ihash(kv.Key) % reducerNum
		hashKV[nthReducer] = append(hashKV[nthReducer], kv)
	}
	for i := 0; i < reducerNum; i++ {
		interFileName := filename + "-" + strconv.Itoa(info.TaskNo) + "-" + strconv.Itoa(i)
		tempFile, _ := os.CreateTemp("", "temp-")
		defer os.Remove(tempFile.Name())

		enc := json.NewEncoder(tempFile)
		for _, kv := range hashKV[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("encode err!")
			}
		}
		err := os.Rename(tempFile.Name(), "../"+interFileName)
		if err != nil {
			log.Fatal("rename err!")
		}
	}
}

func doReduce(reducef func(string, []string) string, info *TaskInfo) {
	filenames := info.FileNames
	nre := info.NReducer
	var intermediate []KeyValue

	//log.Printf("taskFiles length: %v", len(taskFiles))
	for _, file := range filenames {
		f, err := os.Open(file)
		if err != nil {
			log.Fatal("open fail!")
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		f.Close()
	}
	//log.Printf("intermediate length: %v", len(intermediate))
	sort.Sort(ByKey(intermediate))
	tempFile, err := os.CreateTemp("", "temp-"+strconv.Itoa(nre))
	if err != nil {
		log.Fatalf("cannot create tempfile")
	}
	defer os.Remove(tempFile.Name())

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(tempFile.Name(), "./mr-out-"+strconv.Itoa(nre))
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

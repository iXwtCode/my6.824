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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type GetTaskArgs struct {
}
type GetTaskRely struct {
	TaskInfo TaskInfo
}
type TaskInfo struct {
	taskType   TaskType
	taskNo     int
	fileNames  string
	ReducerNum int // 总的reducer数量，map操作中用来计算key对应的reducer。
	nReducer   int // 表明 task 完成那个 reducer 的任务. 在 map 任务中设置为 nil
}
type FinishArgs struct {
	TaskInfo TaskInfo
}
type FinishReply struct {
	Deleted bool
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

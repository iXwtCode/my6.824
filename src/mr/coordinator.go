package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType int8

const (
	Map TaskType = iota
	Reduce
	Wait
	PleaseExit
)

type CoState int8

const (
	Maping CoState = iota
	MapWait
	Reducing
	ReduceWait
	Finish
)

type Coordinator struct {
	// Your definitions here.
	Mu           sync.Mutex
	State        CoState
	Tasks        map[int]TaskInfo
	Mapchan      chan *TaskInfo
	Reducechan   chan *TaskInfo
	TaskFinished map[TaskType]map[int]bool
	NextTaskNo   int
	ReducerNum   int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) TaskNO() int {
	ret := c.NextTaskNo
	c.NextTaskNo += 1
	return ret
}

// 检查 map or reduce 是否完成
func (c *Coordinator) HasFinished(taskType TaskType) bool {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	taskMap := c.TaskFinished[taskType]
	for _, v := range taskMap {
		if !v {
			return false
		}
	}
	return true
}

func checkTimeOut(c *Coordinator, info TaskInfo, ch *chan *TaskInfo) {
	time.Sleep(time.Second * 10)
	taskType := info.taskType
	v, _ := c.TaskFinished[taskType][info.taskNo]
	if !v { // 10s 后未完成
		newTask := info
		newTask.taskNo = c.TaskNO()
		delete(c.TaskFinished[taskType], info.taskNo)
		c.TaskFinished[taskType][info.taskNo] = false
		*ch <- &newTask
	}
}

func (c *Coordinator) Task(args *GetTaskArgs, reply *TaskInfo) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	switch c.State {
	case Maping:
		{
			if len(c.Mapchan) > 0 {
				*reply = *<-c.Mapchan
				fmt.Printf("file name: %v, taskNO: %v\n", reply.fileNames, reply.taskNo)
				go checkTimeOut(c, *reply, &c.Mapchan)
			} else {
				c.State = MapWait
				reply.taskType = Wait
			}
		}
	case MapWait:
		{
			reply.taskType = Wait
			if c.HasFinished(Map) { // map 操作完成
				for i := 0; i < c.ReducerNum; i++ {
					task := TaskInfo{
						taskType:   Reduce,
						taskNo:     c.TaskNO(),
						fileNames:  "../",
						ReducerNum: c.ReducerNum,
						nReducer:   i,
					}
					c.Reducechan <- &task
				}
			}
		}
	case Reducing:
		{
			if len(c.Reducechan) > 0 {
				*reply = *<-c.Reducechan
				go checkTimeOut(c, *reply, &c.Reducechan)
			} else {
				c.State = ReduceWait
				reply.taskType = Wait
			}
		}
	case ReduceWait:
		{
			reply.taskType = Wait
			if c.HasFinished(Reduce) {
				c.State = Finish
			}
		}
	case Finish:
		{
			reply.taskType = Wait
		}
	}
	return nil
}

func (c *Coordinator) Finish(args *TaskInfo, reply *FinishReply) error {
	taskType := args.taskType

	_, ok := c.TaskFinished[taskType][args.taskNo]
	if ok { // 这个任务没有因为超时被删除
		c.Mu.Lock()
		defer c.Mu.Unlock()
		c.TaskFinished[taskType][args.taskNo] = true
		reply.Deleted = false
	} else {
		reply.Deleted = true
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	for _, v := range c.TaskFinished[Reduce] {
		if !v {
			return false
		}
	}
	return len(c.TaskFinished[Reduce]) != 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Mu:           sync.Mutex{},
		State:        Maping,
		Mapchan:      make(chan *TaskInfo, len(files)),
		Reducechan:   make(chan *TaskInfo, nReduce),
		TaskFinished: map[TaskType]map[int]bool{},
		NextTaskNo:   0,
		ReducerNum:   nReduce,
	}
	c.TaskFinished[Map] = make(map[int]bool)
	c.TaskFinished[Reduce] = make(map[int]bool)
	// Your code here.
	for _, file := range files {
		task := TaskInfo{
			taskType:   Map,
			taskNo:     c.TaskNO(),
			fileNames:  file,
			ReducerNum: nReduce,
			nReducer:   -1,
		}
		c.Mapchan <- &task
	}
	c.server()
	return &c
}

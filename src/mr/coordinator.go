package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
	Fmu          sync.Mutex
	State        CoState
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
	//c.Mu.Lock()
	//defer c.Mu.Unlock()
	c.Fmu.Lock()
	defer c.Fmu.Unlock()
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
	taskType := info.TaskType
	v, _ := c.TaskFinished[taskType][info.TaskNo]
	if !v { // 10s 后未完成
		//fmt.Printf("time out: %v\n", info.TaskNo)
		if c.State == MapWait {
			c.State = Maping
		}
		if c.State == ReduceWait {
			c.State = Reducing
		}
		newTask := info
		newTask.TaskNo = c.TaskNO()
		c.Fmu.Lock()
		delete(c.TaskFinished[taskType], info.TaskNo)
		c.TaskFinished[taskType][newTask.TaskNo] = false
		c.Fmu.Unlock()
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
				//fmt.Printf("Maping: %v\n", reply.TaskNo)
				//fmt.Printf("file name: %v, taskNO: %v\n", reply.FileNames, reply.TaskNo)
				go checkTimeOut(c, *reply, &c.Mapchan)
			} else {
				c.State = MapWait
				reply.TaskType = Wait
			}
		}
	case MapWait:
		{
			//fmt.Printf("MapWait: %v\n")
			reply.TaskType = Wait
			if c.HasFinished(Map) { // map 操作完成
				hashFiles := make(map[int][]string)
				files, err := os.ReadDir("../../")
				if err != nil {
					log.Fatal("ReadDir fail!")
				}

				for _, file := range files {
					fname := file.Name()
					sp := strings.Split(fname, "-")
					var taskNo, nr int
					if len(sp) == 4 {
						taskNo, _ = strconv.Atoi(sp[2])
						nr, _ = strconv.Atoi(sp[3])
					} else {
						continue
					}
					if _, ok := c.TaskFinished[Map][taskNo]; ok && strings.HasSuffix(fname, strconv.Itoa(nr)) {
						hashFiles[nr] = append(hashFiles[nr], "../../"+fname)
					}
				}

				for i := 0; i < c.ReducerNum; i++ {
					task := TaskInfo{
						TaskType:   Reduce,
						TaskNo:     c.TaskNO(),
						FileNames:  hashFiles[i],
						ReducerNum: c.ReducerNum,
						NReducer:   i,
					}
					c.Reducechan <- &task
					c.TaskFinished[Reduce][task.TaskNo] = false
				}
				c.State = Reducing
			}
		}
	case Reducing:
		{
			//fmt.Printf("Reducing\n")
			if len(c.Reducechan) > 0 {
				*reply = *<-c.Reducechan
				go checkTimeOut(c, *reply, &c.Reducechan)
			} else {
				c.State = ReduceWait
				reply.TaskType = Wait
			}
		}
	case ReduceWait:
		{
			//fmt.Printf("ReduceWait\n")
			reply.TaskType = Wait
			if c.HasFinished(Reduce) {
				c.State = Finish
				reply.TaskType = PleaseExit
			}
		}
	case Finish:
		{
			//fmt.Printf("Finish\n")
			reply.TaskType = PleaseExit
		}
	}
	return nil
}

func (c *Coordinator) Finish(args *TaskInfo, reply *FinishReply) error {

	//fmt.Printf("call finish: %v\n", args.TaskNo)
	c.Fmu.Lock()
	defer c.Fmu.Unlock()
	//fmt.Printf("call finish lock: %v\n", args.TaskNo)
	taskType := args.TaskType
	_, ok := c.TaskFinished[taskType][args.TaskNo]
	if ok { // 这个任务没有因为超时被删除
		c.TaskFinished[taskType][args.TaskNo] = true
		reply.Deleted = false
		//fmt.Printf("finish: %v\n", args.TaskNo)
	} else {
		//fmt.Printf("deleted: %v\n", args.TaskNo)
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
			TaskType:   Map,
			TaskNo:     c.TaskNO(),
			FileNames:  []string{file},
			ReducerNum: nReduce,
			NReducer:   -1,
		}
		c.Mapchan <- &task
		c.TaskFinished[Map][task.TaskNo] = false
	}
	c.server()
	return &c
}

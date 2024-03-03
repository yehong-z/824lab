package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	state           State
	mu              sync.RWMutex
	mapTaskQueue    chan Task // map的任务队列
	reduceTaskQueue chan Task // reduce 任务队列
	nReduce         int       // 用于哈希
	taskID          int
	tasks           map[int]struct{} // 已经发布任务
	mapNum          int
	reduceNum       int
}

func (c *Coordinator) AddTask(t *Task) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.taskID++
	t.id = c.taskID
	c.tasks[t.id] = struct{}{}

	go func() {
		// 超时后重新加入任务
		time.Sleep(time.Second * 10)
		c.RecoverTask(t)
	}()
}

func (c *Coordinator) RecoverTask(t *Task) {
	c.mu.Lock()
	c.mu.Unlock()
	if _, ok := c.tasks[t.id]; ok {
		if t.tt == TASK_MAP {
			c.mapTaskQueue <- *t
		} else if t.tt == TASK_REDUCE {
			c.reduceTaskQueue <- *t
		}

		delete(c.tasks, t.id)
	}
}

// Your code here -- RPC handlers for the worker to call.

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
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.state == STATE_FINISH {
		return true
	}

	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// 初始化操作
	c.nReduce = nReduce
	c.state = STATE_MAP
	c.tasks = make(map[int]struct{})
	c.mapTaskQueue = make(chan Task, 100)
	c.reduceTaskQueue = make(chan Task, 100)
	c.mapNum = len(files)
	c.reduceNum = nReduce
	for _, file := range files {
		c.mapTaskQueue <- Task{
			file: file,
			tt:   TASK_MAP,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTaskQueue <- Task{
			tt:        TASK_REDUCE,
			reduceNum: i,
		}
	}

	c.server()
	return &c
}

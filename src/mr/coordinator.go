package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	finished  bool
	mu        sync.RWMutex
	taskQueue chan Task // map和reduce的任务队列
	nReduce   int       // 用于哈希
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
	ret := c.finished
	c.mu.RUnlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// 初始化操作
	c.nReduce = nReduce
	c.finished = false
	c.taskQueue = make(chan Task, 100)
	for _, file := range files {
		c.taskQueue <- Task{
			file: file,
			tt:   TASK_MAP,
		}
	}

	c.server()
	return &c
}

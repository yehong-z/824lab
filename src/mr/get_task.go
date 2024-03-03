package mr

import (
	"log"
	"time"
)

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskType TaskType
	File     string
	ID       int
	NReduce  int
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if c.finished {
		reply.TaskType = TASK_CLOSE
		return nil
	}

	select {
	case t := <-c.mapTaskQueue:
		reply.TaskType = t.tt
		reply.File = t.file
		reply.NReduce = c.nReduce
		// 定时，若没有按时提交任务则重新加入任务队列
		c.AddTask(&t)
		reply.ID = t.id
		log.Println("get task ", reply)
	case <-time.Tick(time.Millisecond * 100):
		reply.TaskType = TASK_WAIT
	}

	return nil
}

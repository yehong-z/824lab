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
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if c.finished {
		reply.TaskType = TASK_CLOSE
		return nil
	}

	select {
	case t := <-c.taskQueue:
		reply.TaskType = t.tt
		reply.File = t.file
		log.Println("get task ", t)
	case <-time.Tick(time.Millisecond * 100):
		reply.TaskType = TASK_WAIT
	}

	return nil
}

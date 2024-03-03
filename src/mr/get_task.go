package mr

import (
	"time"
)

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskType  TaskType
	File      string
	ID        int
	NReduce   int
	ReduceNum int
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if c.state == STATE_FINISH {
		reply.TaskType = TASK_CLOSE
		return nil
	} else if c.state == STATE_MAP {
		select {
		case t := <-c.mapTaskQueue:
			reply.TaskType = t.tt
			reply.File = t.file
			reply.NReduce = c.nReduce
			// 定时，若没有按时提交任务则重新加入任务队列
			c.AddTask(&t)
			reply.ID = t.id
			// log.Println("get map task ", reply)
		case <-time.Tick(time.Millisecond * 100):
			reply.TaskType = TASK_WAIT
		}
	} else if c.state == STATE_REDUCE {
		select {
		case t := <-c.reduceTaskQueue:
			reply.TaskType = t.tt
			reply.ReduceNum = t.reduceNum
			// 定时，若没有按时提交任务则重新加入任务队列
			c.AddTask(&t)
			reply.ID = t.id
			// log.Println("get reduce task ", reply)
		case <-time.Tick(time.Millisecond * 100):
			reply.TaskType = TASK_WAIT
		}
	}

	return nil
}

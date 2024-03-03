package mr

type FinishTaskArgs struct {
	ID   int
	Type TaskType
}

type FinishTaskReply struct {
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Type == TASK_MAP {
		c.mapNum--
		if c.mapNum == 0 {
			c.state = STATE_REDUCE
		}

	} else if args.Type == TASK_REDUCE {
		c.reduceNum--
		if c.reduceNum == 0 {
			c.state = STATE_FINISH
		}
	}
	delete(c.tasks, args.ID)

	return nil
}

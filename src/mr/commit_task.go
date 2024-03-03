package mr

type CommitTaskArgs struct {
	Type TaskType
	File string
	ID   int
}

type CommitTaskReply struct {
	Accept bool
}

func (c *Coordinator) CommitTask(args *CommitTaskArgs, reply *CommitTaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tasks[args.ID]; ok {
		reply.Accept = true

	} else {
		reply.Accept = false
	}

	return nil
}

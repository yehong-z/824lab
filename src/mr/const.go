package mr

type TaskType int

const (
	TASK_MAP    = 1
	TASK_REDUCE = 2
	TASK_WAIT   = 3
	TASK_CLOSE  = 4
)

const (
	MR_OUTPUT = "mr-out-"
)

const (
	RPC_ERROR = "rpc error"
)

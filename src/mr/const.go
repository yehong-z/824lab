package mr

type TaskType int

const (
	TASK_MAP    = 1
	TASK_REDUCE = 2
	TASK_WAIT   = 3
	TASK_CLOSE  = 4
)

type State int

const (
	STATE_FINISH = 0
	STATE_MAP    = 1
	STATE_REDUCE = 2
)

const (
	COMMIT_MAP    = 1
	COMMIT_REDUCE = 2
)

const (
	REDUCE_INTUT = "mr-tmp-"
	MR_OUTPUT    = "mr-out-"
)

const (
	RPC_ERROR = "rpc error"
)

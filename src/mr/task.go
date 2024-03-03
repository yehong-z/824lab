package mr

type Task struct {
	id        int
	tt        TaskType
	file      string
	reduceNum int
}

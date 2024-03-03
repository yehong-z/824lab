package mr

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// pid := os.Getpid()

	for {
		task, err := GetTask()
		if err != nil {
			log.Fatal("get task error")
		}

		if task.TaskType == TASK_MAP {
			//log.Println("run map")
			cont, err := GetContent(task.File)
			if err != nil {
				continue
			}
			KV := mapf(task.File, cont)

			res, _ := CommitTask(task.ID, task.TaskType, task.File)
			if res.Accept == true {
				// TODO 将结果写入文件
				kkv := make(map[int][]KeyValue)
				for _, kv := range KV {
					hashKey := ihash(kv.Key) % task.NReduce
					kkv[hashKey] = append(kkv[hashKey], kv)
				}

				for hashKey, KVList := range kkv {
					var b strings.Builder
					for _, kvpair := range KVList {
						fmt.Fprintf(&b, "%v %v\n", kvpair.Key, kvpair.Value)
					}

					newFilename := fmt.Sprintf("%v%v", REDUCE_INTUT, hashKey)
					outputFile, _ := os.OpenFile(newFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						fmt.Println("Error creating file:", err)
						return
					}

					fmt.Fprintf(outputFile, b.String())
					outputFile.Close()
				}
			}
			_ = FinishTask(task.TaskType, task.ID)

		} else if task.TaskType == TASK_REDUCE {
			//log.Println("run reduce")
			// 完成reduce任务
			filename := fmt.Sprintf("%v%v", REDUCE_INTUT, task.ReduceNum)
			contents, err := GetContent(filename)
			if err != nil {
				continue
			}
			ff := func(r rune) bool {
				return unicode.IsSpace(r)
			}
			words := strings.FieldsFunc(contents, ff)

			intermediate := make(map[string][]string)
			reduceResult := make(map[string]string)
			for i := 0; i < len(words); i += 2 {
				key := words[i]
				value := words[i+1]
				intermediate[key] = append(intermediate[key], value)
			}

			for k, v := range intermediate {
				reduceResult[k] = reducef(k, v)
			}

			res, _ := CommitTask(task.ID, task.TaskType, task.File)
			if res.Accept == true {
				var b strings.Builder
				for resultK, resultV := range reduceResult {
					fmt.Fprintf(&b, "%v %v\n", resultK, resultV)
				}

				newFilename := fmt.Sprintf("%v%v", MR_OUTPUT, task.ReduceNum)
				outputFile, _ := os.OpenFile(newFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					fmt.Println("Error creating file:", err)
					return
				}

				fmt.Fprintf(outputFile, b.String())
				outputFile.Close()
			}
			FinishTask(task.TaskType, task.ID)

		} else if task.TaskType == TASK_WAIT {
			//log.Println("empty task wait")
			time.Sleep(time.Millisecond * 100)
		} else if task.TaskType == TASK_CLOSE {
			// log.Println("connect close")
			break
		} else {
			log.Println("default ", task)
			break
		}
	}

	return
}

// 从调度器获取任务
func GetTask() (*GetTaskReply, error) {
	re := &GetTaskReply{}
	ok := call("Coordinator.GetTask", &GetTaskArgs{}, re)
	if ok {
		// log.Println(re)
		return re, nil
	} else {
		return nil, errors.New(RPC_ERROR)
	}

}

// 提交任务
func CommitTask(id int, t TaskType, file string) (*CommitTaskReply, error) {
	args := &CommitTaskArgs{
		ID:   id,
		Type: t,
		File: file,
	}
	re := &CommitTaskReply{}
	ok := call("Coordinator.CommitTask", args, re)
	if ok {
		//log.Println(re)
		return re, nil
	} else {
		return nil, errors.New(RPC_ERROR)
	}
}

// 落盘后再提交
func FinishTask(t TaskType, id int) error {
	args := &FinishTaskArgs{
		ID:   id,
		Type: t,
	}
	re := &FinishTaskReply{}
	ok := call("Coordinator.FinishTask", args, re)
	if ok {
		return nil
	} else {
		return errors.New(RPC_ERROR)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type SortedKey []KeyValue

// for sorting by key.
func (a SortedKey) Len() int           { return len(a) }
func (a SortedKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortedKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	//CallExample()
	flag := true
	for flag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)

				fmt.Println("Handling the map task......")

				CallTaskDone(&task)

			}
		case WaitingTask:
			{
				fmt.Println("waiting task......")
				time.Sleep(time.Second)
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task)

				fmt.Println("Handling the reduce task......")
				CallTaskDone(&task)
			}
		case DoneTask:
			{
				fmt.Println("job all done......")
				flag = false
			}

		}
	}

}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	// 定义好输出的中间值
	intermediate := []KeyValue{}
	// 获取输入文件名
	fileName := task.FileName[0]

	// 打开文件
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("[DoMapTask] open file error=%v \n", err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("[DoMapTask] read file error=%v \n", err)
	}
	_ = file.Close()
	intermediate = mapf(fileName, string(content))
	// 接下来是输出到一个中间值文件
	rn := task.ReduceNum

	// 映射过程：每个文件会映射到mr-temp-taskId-rn这样的中间文件
	hashKV := make([][]KeyValue, rn)
	for _, kv := range intermediate {
		hashKV[ihash(kv.Key)%rn] = append(hashKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		tempFileName := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		tempFile, _ := os.Create(tempFileName)
		enc := json.NewEncoder(tempFile)
		for _, kv := range hashKV[i] {
			_ = enc.Encode(kv)
		}
		_ = tempFile.Close()
	}

}

func DoReduceTask(reducef func(string, []string) string, task *Task) {
	filenames := task.FileName
	intermediateKV := shuffle(filenames)
	reduceTaskId := task.TaskId
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-out-")
	if err != nil {
		log.Fatal("[DoReduceTask] Failed to create temp file", err)
	}
	i := 0
	for i < len(intermediateKV) {
		j := i + 1
		for j < len(intermediateKV) && intermediateKV[j].Key == intermediateKV[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediateKV[k].Value)
		}
		output := reducef(intermediateKV[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediateKV[i].Key, output)

		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%v", reduceTaskId)
	os.Rename(tempFile.Name(), fn)
}

func shuffle(filenames []string) []KeyValue {
	var intermediateKV []KeyValue
	for _, filenames := range filenames {
		file, _ := os.Open(filenames)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediateKV = append(intermediateKV, kv)
		}
		_ = file.Close()
	}
	sort.Sort(SortedKey(intermediateKV))
	return intermediateKV
}

func CallTaskDone(task *Task) Task {
	// 定义rpc请求体
	args := task

	reply := Task{}
	ok := call("Coordinator.MarkTaskDone", args, &reply)
	if ok {
		fmt.Printf("[CallTaskDone] success! reply=%v \n", reply)
	} else {
		fmt.Println("[CallTaskDone] fail!")
	}
	return reply
}

// GetTask 获取任务
func GetTask() Task {
	// 定义rpc请求体
	args := TaskRequest{}
	task := Task{}
	ok := call("Coordinator.PollTask", &args, &task)
	if ok {
		fmt.Printf("[GetTask]successfully get task back from coordinator, task = %v \n", task)
	} else {
		fmt.Println("[GetTask]fail to get task back from coordinator")
	}
	return task
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
		log.Fatal("[call]dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
